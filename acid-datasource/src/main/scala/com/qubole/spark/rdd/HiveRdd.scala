/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qubole.spark.rdd

import java.io._
import java.net.{URL, URLClassLoader}
import java.text.SimpleDateFormat
import java.util.{Date, Locale, Properties}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.hive.ql.io.orc.{OrcSerde, OrcSplit}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorConverters, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapred.lib.CombineFileSplit
import org.apache.hadoop.mapreduce.TaskType
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.util.ReflectionUtils
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rdd._
import org.apache.spark.scheduler.{HDFSCacheTaskLocation, HostTaskLocation}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SpecificInternalRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import com.qubole.spark.rdd.HiveRDD.{logDebug, toCatalystDecimal, unwrapperFor}
import com.qubole.spark.util.{SerializableConfiguration, Util}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.MutableURLClassLoader

import scala.collection.immutable.Map
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.ClassTag
import scala.util.Try

/*
 * This class helps in speeding up split computation for SparkSQL queries.
 *
 * When a SELECT query runs on a partitioned table, a HadoopTableReader is
 * created and it creates a HadoopRDD per partition dir. At this point,
 * we make sure all the HadoopRDDs point to the same FileStatusCache object.
 *
 * After the creation of RDDs, the dependencies of the RDDs are discovered.
 * In that phase, the list of files in each input dir and the FileStatus
 * objects for those files are picked up from the cache rather than
 * making listStatus calls per input dir. The first RDD's getPartitions
 * call triggers actual population of the cache by allowing the IPP code
 * to discover all the files based on all the input partition dirs.
 */

private[spark] class HiveRDD(
                       sc: SparkContext,
                       broadcastedConf: Broadcast[SerializableConfiguration],
                       initLocalJobConfFuncOpt: Option[JobConf => Unit],
                       inputFormatClass: Class[_ <: InputFormat[Writable, Writable]],
                       minPartitions: java.lang.Integer,
                       deserializerClassName: String,
                       mutableRow: SpecificInternalRow,
                       attrsWithIndex: Seq[(Attribute, Int)],
                       tableProps: Properties, schemaColNames: String, schemaColTypes: String
             )
  extends RDD[InternalRow](sc, Nil) with Logging {

//  // The getPartitions call and split computation happens only in the driver.
//  // So making it transient should not cause any problems in the executors.
//  @transient var fileStatusCache : Option[FileStatusCache] = None
//
//  def setFileStatusCache(fscache: FileStatusCache): Unit = {
//    fileStatusCache = Some(fscache)
//  }

  def getInputFormatClassString: String = {
    if (inputFormatClass.getName == "org.apache.hadoop.hive.ql.io.orc.OrcInputFormats") {
      return "org.apache.hadoop.hive.ql.io.HiveInputFormat"
    } else {
      return inputFormatClass.getName
    }
  }

  // TODO:
//  if (initLocalJobConfFuncOpt.isDefined) {
//    sparkContext.clean(initLocalJobConfFuncOpt.get)
//  }


  protected val jobConfCacheKey: String = "rdd_%d_job_conf".format(id)

  protected val inputFormatCacheKey: String = "rdd_%d_input_format".format(id)

  // used to build JobTracker ID
  private val createTime = new Date()

  private val shouldCloneJobConf = sparkContext.getConf.getBoolean("spark.hadoop.cloneConf", false)

  private val ignoreCorruptFiles = sparkContext.getConf.getBoolean("spark.files.ignoreCorruptFiles", false)

  private val ignoreMissingFiles = sparkContext.getConf.getBoolean("spark.files.ignoreMissingFiles", false)

  private val ignoreEmptySplits = sparkContext.getConf.getBoolean("spark.hadoopRDD.ignoreEmptySplits", false)

  // Returns a JobConf that will be used on slaves to obtain input splits for Hadoop reads.
  protected def getJobConf(): JobConf = {
    val conf: Configuration = broadcastedConf.value.value
    val jConf = if (shouldCloneJobConf) {
      // Hadoop Configuration objects are not thread-safe, which may lead to various problems if
      // one job modifies a configuration while another reads it (SPARK-2546).  This problem occurs
      // somewhat rarely because most jobs treat the configuration as though it's immutable.  One
      // solution, implemented here, is to clone the Configuration object.  Unfortunately, this
      // clone can be very expensive.  To avoid unexpected performance regressions for workloads and
      // Hadoop versions that do not suffer from these thread-safety issues, this cloning is
      // disabled by default.
      HiveRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
        logDebug("Cloning Hadoop Configuration")
        val newJobConf = new JobConf(conf)
        if (!conf.isInstanceOf[JobConf]) {
          initLocalJobConfFuncOpt.foreach(f => f(newJobConf))
        }
        newJobConf
      }
    } else {
      if (conf.isInstanceOf[JobConf]) {
        logDebug("Re-using user-broadcasted JobConf")
        conf.asInstanceOf[JobConf]
      } else {
        Option(HiveRDD.getCachedMetadata(jobConfCacheKey))
          .map { conf =>
            logDebug("Re-using cached JobConf")
            conf.asInstanceOf[JobConf]
          }
          .getOrElse {
            // Create a JobConf that will be cached and used across this RDD's getJobConf() calls in
            // the local process. The local cache is accessed through HiveRDD.putCachedMetadata().
            // The caching helps minimize GC, since a JobConf can contain ~10KB of temporary
            // objects. Synchronize to prevent ConcurrentModificationException (SPARK-1097,
            // HADOOP-10456).
            HiveRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
              logDebug("Creating new JobConf and caching it for later re-use")
              val newJobConf = new JobConf(conf)
              initLocalJobConfFuncOpt.foreach(f => f(newJobConf))
              HiveRDD.putCachedMetadata(jobConfCacheKey, newJobConf)
              newJobConf
            }
          }
      }
    }
    jConf.set("schema.evolution.columns", schemaColNames)
    jConf.set("schema.evolution.columns.types", schemaColTypes)
    jConf
  }

  def withHiveState[A](f: => A): A = {
    val hiveMetastoreJars = "/Users/prakharj/src/hadoop2/hadoop-dist/target/hadoop-2.6.0-qds-0.4.22-SNAPSHOT/etc/hadoop:/Users/prakharj/src/hadoop2/hadoop-dist/target/hadoop-2.6.0-qds-0.4.22-SNAPSHOT/share/hadoop/common/lib/*:/Users/prakharj/src/hadoop2/hadoop-dist/target/hadoop-2.6.0-qds-0.4.22-SNAPSHOT/share/hadoop/common/*:/Users/prakharj/src/hadoop2/hadoop-dist/target/hadoop-2.6.0-qds-0.4.22-SNAPSHOT/share/hadoop/hdfs:/Users/prakharj/src/hadoop2/hadoop-dist/target/hadoop-2.6.0-qds-0.4.22-SNAPSHOT/share/hadoop/hdfs/lib/*:/Users/prakharj/src/hadoop2/hadoop-dist/target/hadoop-2.6.0-qds-0.4.22-SNAPSHOT/share/hadoop/hdfs/*:/Users/prakharj/src/hadoop2/hadoop-dist/target/hadoop-2.6.0-qds-0.4.22-SNAPSHOT/share/hadoop/yarn/lib/*:/Users/prakharj/src/hadoop2/hadoop-dist/target/hadoop-2.6.0-qds-0.4.22-SNAPSHOT/share/hadoop/yarn/*:/Users/prakharj/src/hadoop2/hadoop-dist/target/hadoop-2.6.0-qds-0.4.22-SNAPSHOT/share/hadoop/mapreduce/*:/share/hadoop/tools:/Users/prakharj/src/hadoop2/hadoop-dist/target/hadoop-2.6.0-qds-0.4.22-SNAPSHOT/share/hadoop/tools/lib/*:/Users/prakharj/src/hadoop2/hadoop-dist/target/hadoop-2.6.0-qds-0.4.22-SNAPSHOT/share/hadoop/tools/*:/share/hadoop/qubole:/Users/prakharj/src/hadoop2/hadoop-dist/target/hadoop-2.6.0-qds-0.4.22-SNAPSHOT/share/hadoop/qubole/*:/contrib/capacity-scheduler/*.jar:/Users/prakharj/src/backups/jarshive3/*:/Users/prakharj/src/backups/jarshive3extras/*"
    val jars =
      hiveMetastoreJars
        .split(File.pathSeparator)
        .flatMap {
          case path if new File(path).getName == "*" =>
            val files = new File(path).getParentFile.listFiles()
            if (files == null) {
              logWarning(s"Hive jar path '$path' does not exist.")
              Nil
            } else {
              files.filter(_.getName.toLowerCase(Locale.ROOT).endsWith(".jar"))
            }
          case path =>
            new File(path) :: Nil
        }
        .map(_.toURI.toURL)

    val loader = new IsolatedClientLoaderHive(
      execJars = jars.toSeq,
      //sharedPrefixes = Seq("org.apache.hadoop.hive.ql.io.orc.OrcStruct"),
      isolationOn = true)

    val original = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(loader.classLoader)
    val ret = try f finally {
      Thread.currentThread().setContextClassLoader(original)
    }
    ret
  }

  protected def getInputFormat(conf: JobConf): InputFormat[Writable, Writable] = {
    val newInputFormat = {
      val newInputFormatClassString = getInputFormatClassString
      val newInputFormatClass = Util.classForName(newInputFormatClassString)
      val newInputFormat = ReflectionUtils.newInstance(
        newInputFormatClass.asInstanceOf[Class[_]], conf)
        .asInstanceOf[InputFormat[Writable, Writable]]
      newInputFormat match {
        case c: Configurable => c.setConf(conf)
        case _ =>
      }
      newInputFormat
    }
    newInputFormat
  }

  private def getBytesFromInputSplit(obj: Object): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    try {
      val out = new ObjectOutputStream(bos)
      out.writeObject(obj)
      out.flush()
      val bytes = bos.toByteArray()
      bytes
    } finally {
      try {
        bos.close();
      } catch {
        case e: IOException =>
          logError("exception", e)
      }
    }
  }

  override def getPartitions: Array[Partition] = {
    val jobConf = getJobConf()
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    try {
      // Set the cache on the JobConf before calling getSplits. This will
      // make sure that the listStatus calls will lookup cached information
      // instead of making S3 calls. The JobConf is specific to this RDD
      // because of the way getJobConf works.
//      fileStatusCache.foreach(jobConf.setFileStatusCache(_))
      val inputF = getInputFormat(jobConf)
      jobConf.set("hive.execution.engine", "mr")
      jobConf.set("has.map.work", "true")
      val allInputSplits = inputF.getSplits(jobConf, minPartitions.toInt)
      val inputSplits = if (ignoreEmptySplits) {
        allInputSplits.filter(_.getLength > 0)
      } else {
        allInputSplits
      }
      logInfo(s"Total splits - ${inputSplits.size}")
      val array = new Array[Partition](inputSplits.size)
      for (i <- 0 until inputSplits.size) {
        array(i) = new HivePartition(id, i, getBytesFromInputSplit(
          new SerializableWritable[InputSplit](inputSplits(i))
        ))
      }
      array
    } catch {
      case e: InvalidInputException if ignoreMissingFiles =>
        logWarning(s"${jobConf.get(FileInputFormat.INPUT_DIR)} doesn't exist and no" +
          s" partitions returned from this path.", e)
        Array.empty[Partition]
    }
  }

    override def compute(theSplit: Partition,
                         context: TaskContext): InterruptibleIterator[InternalRow] = {
      val jConf = getJobConf()
      jConf.setBoolean("hive.transactional.table.scan", true)
      val obj = new HiveRddHelper(theSplit, jConf, getInputFormatClassString, createTime,
        ignoreCorruptFiles.toString,
        ignoreMissingFiles.toString, deserializerClassName,
        broadcastedConf.value.value, tableProps,
        mutableRow, attrsWithIndex, context)
      obj.compute()
    }

//  override def compute(theSplit: Partition,
//                       context: TaskContext): InterruptibleIterator[InternalRow] = {
//    withHiveState {
//    logInfo("------------------------------------ Checkpoint -3")
//    val iter = new NextIterator[(Writable, Writable)] {
//
//      private val split = theSplit.asInstanceOf[HadoopPartition]
//      logInfo("Input split: " + split.inputSplit)
//      val scheme = Utils.getSplitScheme(split.inputSplit.value)
//      val jobConf = getJobConf()
//
//      val inputMetrics = context.taskMetrics().inputMetrics
//      val existingBytesRead = inputMetrics.bytesRead
//      val blobStoreInputMetrics: Option[InputMetrics] = if (scheme == SplitFileSystemType.BLOB_STORE ) {
//        Some(context.taskMetrics().blobStoreInputMetrics)
//      } else {
//        None
//      }
//      val existingBlobStoreBytesRead = blobStoreInputMetrics.map(_.bytesRead).sum
//
//      // Sets InputFileBlockHolder for the file block's information
//      split.inputSplit.value match {
//        case fs: FileSplit =>
//          InputFileBlockHolder.set(fs.getPath.toString, fs.getStart, fs.getLength)
//        case _ =>
//          InputFileBlockHolder.unset()
//      }
//
//      //      if (split.inputSplit.value.isInstanceOf[org.apache.hadoop.hive.ql.io.orc.OrcSplit]) {
//      //        logWarning(s"Total delta file in split: " +
//      //          s"${split.inputSplit.value.asInstanceOf[org.apache.hadoop.hive.ql.io.orc.OrcSplit].getDeltas}")
//      //      }
//      //
//      // Find a function that will return the FileSystem bytes read by this thread. Do this before
//      // creating RecordReader, because RecordReader's constructor might read some bytes
//      private val getBytesReadCallback: Option[() => Long] = split.inputSplit.value match {
//        case _: FileSplit | _: CombineFileSplit =>
//          SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
//        case _ => None
//      }
//
//      // We get our input bytes from thread-local Hadoop FileSystem statistics.
//      // If we do a coalesce, however, we are likely to compute multiple partitions in the same
//      // task and in the same thread, in which case we need to avoid override values written by
//      // previous partitions (SPARK-13071).
//      private def updateBytesRead(): Unit = {
//        getBytesReadCallback.foreach { getBytesRead =>
//          inputMetrics.setBytesRead(existingBytesRead + getBytesRead())
//          blobStoreInputMetrics.foreach(_.setBytesRead(existingBlobStoreBytesRead + getBytesRead()))
//        }
//      }
//
//      private var reader: RecordReader[Writable, Writable] = null
//      private val inputFormat = getInputFormat(jobConf)
//      HiveRDD.addLocalConfiguration(
//        new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(createTime),
//        context.stageId, theSplit.index, context.attemptNumber, jobConf)
//
//      reader =
//        try {
//          inputFormat.getRecordReader(split.inputSplit.value, jobConf, Reporter.NULL)
//        } catch {
//          case e: FileNotFoundException if ignoreMissingFiles =>
//            logWarning(s"Skipped missing file: ${split.inputSplit}", e)
//            finished = true
//            null
//          // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
//          case e: FileNotFoundException if !ignoreMissingFiles => throw e
//          case e: IOException if ignoreCorruptFiles =>
//            logWarning(s"Skipped the rest content in the corrupted file: ${split.inputSplit}", e)
//            finished = true
//            null
//        }
//      // Register an on-task-completion callback to close the input stream.
//      context.addTaskCompletionListener[Unit] { context =>
//        // Update the bytes read before closing is to make sure lingering bytesRead statistics in
//        // this thread get correctly added.
//        updateBytesRead()
//        closeIfNeeded()
//      }
//
//      private val key: Writable = if (reader == null) {
//        null.asInstanceOf[Writable]
//      } else {
//        reader.createKey()
//      }
//      private val value: Writable = if (reader == null) {
//        null.asInstanceOf[Writable]
//      } else {
//        reader.createValue()
//      }
//
//      override def getNext(): (Writable, Writable) = {
//        logInfo("------------------------------------ Checkpoint getNext")
//        try {
//          finished = !reader.next(key, value)
//        } catch {
//          case e: FileNotFoundException if ignoreMissingFiles =>
//            logWarning(s"Skipped missing file: ${split.inputSplit}", e)
//            finished = true
//          // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
//          case e: FileNotFoundException if !ignoreMissingFiles => throw e
//          case e: IOException if ignoreCorruptFiles =>
//            logWarning(s"Skipped the rest content in the corrupted file: ${split.inputSplit}", e)
//            finished = true
//        }
//        if (!finished) {
//          inputMetrics.incRecordsRead(1)
//          blobStoreInputMetrics.foreach(_.incRecordsRead(1))
//        }
//        if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
//          updateBytesRead()
//        }
//        (key, value)
//      }
//
//      override def close(): Unit = {
//        if (reader != null) {
//          InputFileBlockHolder.unset()
//          try {
//            reader.close()
//          } catch {
//            case e: Exception =>
//              if (!ShutdownHookManager.inShutdown()) {
//                logWarning("Exception in RecordReader.close()", e)
//              }
//          } finally {
//            reader = null
//          }
//          if (getBytesReadCallback.isDefined) {
//            updateBytesRead()
//          } else if (split.inputSplit.value.isInstanceOf[FileSplit] ||
//            split.inputSplit.value.isInstanceOf[CombineFileSplit]) {
//            // If we can't get the bytes read from the FS stats, fall back to the split size,
//            // which may be inaccurate.
//            try {
//              inputMetrics.incBytesRead(split.inputSplit.value.getLength)
//              blobStoreInputMetrics.foreach(_.incBytesRead(split.inputSplit.value.getLength))
//            } catch {
//              case e: java.io.IOException =>
//                logWarning("Unable to get input size to set InputMetrics for task", e)
//            }
//          }
//        }
//      }
//    }
//      logInfo("------------------------------------ Checkpoint 1")
//      import org.apache.hadoop.hive.serde2.Deserializer
//      logInfo("------------------------------------ Checkpoint 2")
//
//
//      val deserializerClassNew = Utils.classForName(
//      deserializerClassName)
//      val deserializer1 = deserializerClassNew.newInstance()
//      // val apple = deserializer1.asInstanceOf[Deserializer]
//      val deserializer = new OrcSerde()
//      logInfo("------------------------------------ Checkpoint 3")
//      deserializer.initialize(broadcastedConf.value.value, tableProps)
//      logInfo("------------------------------------ Checkpoint 4")
//      val rawDeser = deserializer
//      val tableDeser = deserializer
//      val iterator = iter.map(_._2)
//
//      logInfo("------------------------------------ Checkpoint 5")
//      def fillObject(nonPartitionKeyAttrs: Seq[(Attribute, Int)],
//                     mutableRow: InternalRow): Iterator[InternalRow] = {
//
//        val soi = if (rawDeser.getObjectInspector.equals(tableDeser.getObjectInspector)) {
//          rawDeser.getObjectInspector.asInstanceOf[StructObjectInspector]
//        } else {
//          ObjectInspectorConverters.getConvertedOI(
//            rawDeser.getObjectInspector,
//            tableDeser.getObjectInspector).asInstanceOf[StructObjectInspector]
//        }
//
//        logDebug(soi.toString)
//
//        val (fieldRefs, fieldOrdinals) = nonPartitionKeyAttrs.map { case (attr, ordinal) =>
//          soi.getStructFieldRef(attr.name) -> ordinal
//        }.toArray.unzip
//
//        /**
//          * Builds specific unwrappers ahead of time according to object inspector
//          * types to avoid pattern matching and branching costs per row.
//          */
//        val unwrappers: Seq[(Any, InternalRow, Int) => Unit] = fieldRefs.map {
//          _.getFieldObjectInspector match {
//            case oi: BooleanObjectInspector =>
//              (value: Any, row: InternalRow, ordinal: Int) => row.setBoolean(ordinal, oi.get(value))
//            case oi: ByteObjectInspector =>
//              (value: Any, row: InternalRow, ordinal: Int) => row.setByte(ordinal, oi.get(value))
//            case oi: ShortObjectInspector =>
//              (value: Any, row: InternalRow, ordinal: Int) => row.setShort(ordinal, oi.get(value))
//            case oi: IntObjectInspector =>
//              (value: Any, row: InternalRow, ordinal: Int) => row.setInt(ordinal, oi.get(value))
//            case oi: LongObjectInspector =>
//              (value: Any, row: InternalRow, ordinal: Int) => row.setLong(ordinal, oi.get(value))
//            case oi: FloatObjectInspector =>
//              (value: Any, row: InternalRow, ordinal: Int) => row.setFloat(ordinal, oi.get(value))
//            case oi: DoubleObjectInspector =>
//              (value: Any, row: InternalRow, ordinal: Int) => row.setDouble(ordinal, oi.get(value))
//            case oi: HiveVarcharObjectInspector =>
//              (value: Any, row: InternalRow, ordinal: Int) =>
//                row.update(ordinal, UTF8String.fromString(oi.getPrimitiveJavaObject(value).getValue))
//            case oi: HiveCharObjectInspector =>
//              (value: Any, row: InternalRow, ordinal: Int) =>
//                row.update(ordinal, UTF8String.fromString(oi.getPrimitiveJavaObject(value).getValue))
//            case oi: HiveDecimalObjectInspector =>
//              (value: Any, row: InternalRow, ordinal: Int) =>
//                row.update(ordinal, toCatalystDecimal(oi, value))
//            case oi: TimestampObjectInspector =>
//              (value: Any, row: InternalRow, ordinal: Int) =>
//                row.setLong(ordinal, DateTimeUtils.fromJavaTimestamp(
//                  oi.getPrimitiveJavaObject(value).toSqlTimestamp))
//            case oi: DateObjectInspector =>
//              (value: Any, row: InternalRow, ordinal: Int) =>
//                val y = oi.getPrimitiveWritableObject(value).get().toEpochMilli
//                row.setInt(ordinal, DateTimeUtils.fromJavaDate(new java.sql.Date(y)))
//            case oi: BinaryObjectInspector =>
//              (value: Any, row: InternalRow, ordinal: Int) =>
//                row.update(ordinal, oi.getPrimitiveJavaObject(value))
//            case oi =>
//              val unwrapper = unwrapperFor(oi)
//              (value: Any, row: InternalRow, ordinal: Int) => row(ordinal) = unwrapper(value)
//          }
//        }
//
//        val converter = ObjectInspectorConverters.getConverter(rawDeser.getObjectInspector, soi)
//
//        // Map each tuple to a row object
//        iterator.map { value =>
//          val raw = converter.convert(rawDeser.deserialize(value))
//          var i = 0
//          val length = fieldRefs.length
//          while (i < length) {
//            val fieldValue = soi.getStructFieldData(raw, fieldRefs(i))
//            if (fieldValue == null) {
//              mutableRow.setNullAt(fieldOrdinals(i))
//            } else {
//              unwrappers(i)(fieldValue, mutableRow, fieldOrdinals(i))
//            }
//            i += 1
//          }
//
//          mutableRow: InternalRow
//        }
//      }
//
//      logInfo("------------------------------------ Checkpoint 6")
//      val newIter = fillObject(attrsWithIndex, mutableRow)
//      logInfo("------------------------------------ Checkpoint 7")
//      new InterruptibleIterator[InternalRow](context, newIter)
//    }
//  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  override def persist(storageLevel: StorageLevel): this.type = {
    if (storageLevel.deserialized) {
      logWarning("Caching HiveRDDs as deserialized objects usually leads to undesired" +
        " behavior because Hadoop's RecordReader reuses the same Writable object for all records." +
        " Use a map transformation to make copies of the records.")
    }
    super.persist(storageLevel)
  }

  def getConf: Configuration = getJobConf()
}

private[spark] object HiveRDD extends Hive3Inspectors with Logging {


  /**
    * Configuration's constructor is not threadsafe (see SPARK-1097 and HADOOP-10456).
    * Therefore, we synchronize on this lock before calling new JobConf() or new Configuration().
    */
  val CONFIGURATION_INSTANTIATION_LOCK = new Object()

  /** Update the input bytes read metric each time this number of records has been read */
  val RECORDS_BETWEEN_BYTES_READ_METRIC_UPDATES = 256

  /**
    * The three methods below are helpers for accessing the local map, a property of the SparkEnv of
    * the local process.
    */
  def getCachedMetadata(key: String): Any = {
    // SparkEnv.get.hadoopJobMetadata.get(key)
    // TODO:
    None
  }

  private def putCachedMetadata(key: String, value: Any): Unit = {
//    SparkEnv.get.hadoopJobMetadata.put(key, value)
  }

  /** Add Hadoop configuration specific to a single partition and attempt. */
  def addLocalConfiguration(jobTrackerId: String, jobId: Int, splitId: Int, attemptId: Int,
                            conf: JobConf) {
    val jobID = new JobID(jobTrackerId, jobId)
    val taId = new TaskAttemptID(new TaskID(jobID, TaskType.MAP, splitId), attemptId)

    conf.set("mapreduce.task.id", taId.getTaskID.toString)
    conf.set("mapreduce.task.attempt.id", taId.toString)
    conf.setBoolean("mapreduce.task.ismap", true)
    conf.setInt("mapreduce.task.partition", splitId)
    conf.set("mapreduce.job.id", jobID.toString)
  }

  private[spark] def convertSplitLocationInfo(
    infos: Array[SplitLocationInfo]): Option[Seq[String]] = {
    Option(infos).map(_.flatMap { loc =>
      val locationStr = loc.getLocation
      if (locationStr != "localhost") {
        // TODO:
//        if (loc.isInMemory) {
//          logDebug(s"Partition $locationStr is cached by Hadoop.")
//          Some(HDFSCacheTaskLocation(locationStr).toString)
//        } else {
//          Some(HostTaskLocation(locationStr).toString)
//        }
        Some(locationStr)
      } else {
        None
      }
    })
  }
}


class IsolatedClientLoaderHive(
                            val execJars: Seq[URL] = Seq.empty,
                            val isolationOn: Boolean = true,
                            val sharesHadoopClasses: Boolean = true,
                            val rootClassLoader: ClassLoader = ClassLoader.getSystemClassLoader.getParent,
                            val baseClassLoader: ClassLoader = Thread.currentThread().getContextClassLoader,
                            val sharedPrefixes: Seq[String] = Seq.empty,
                            val barrierPrefixes: Seq[String] = Seq.empty)
  extends Logging {

  // Check to make sure that the root classloader does not know about Hive.
  assert(Try(rootClassLoader.loadClass("org.apache.hadoop.hive.conf.HiveConf")).isFailure)

  /** All jars used by the hive specific classloader. */
  private def allJars = execJars.toArray

  def isSharedClass(name: String): Boolean = {
    val isHadoopClass =
      name.startsWith("org.apache.hadoop.") && !name.startsWith("org.apache.hadoop.hive.")

    !name.startsWith("org.apache.spark.sql.hive3") && (
      name.startsWith("org.slf4j") ||
        name.startsWith("org.apache.log4j") || // log4j1.x
        name.startsWith("org.apache.logging.log4j") || // log4j2
        name.startsWith("org.apache.derby.") ||
        name.startsWith("org.apache.spark.") ||
        (sharesHadoopClasses && isHadoopClass) ||
        name.startsWith("scala.") ||
        (name.startsWith("com.google") && !name.startsWith("com.google.cloud")) ||
        name.startsWith("java.lang.") ||
        name.startsWith("java.net") ||
        sharedPrefixes.exists(name.startsWith)
      )
  }

  /** True if `name` refers to a spark class that must see specific version of Hive. */
  def isBarrierClass(name: String): Boolean = barrierPrefixes.exists(name.startsWith)

  def classToPath(name: String): String =
    name.replaceAll("\\.", "/") + ".class"

  /**
    * The classloader that is used to load an isolated version of Hive.
    * This classloader is a special URLClassLoader that exposes the addURL method.
    * So, when we add jar, we can add this new jar directly through the addURL method
    * instead of stacking a new URLClassLoader on top of it.
    */
  val classLoader: MutableURLClassLoader = {
    val isolatedClassLoader =
      if (isolationOn) {
        new URLClassLoader(allJars, rootClassLoader) {
          override def loadClass(name: String, resolve: Boolean): Class[_] = {
            val loaded = findLoadedClass(name)
            val l = if (loaded == null) doLoadClass(name, resolve) else loaded
//            if (name.startsWith("org.apache.hadoop.hive")) {
//              if (l != null && l.getClassLoader != null) {
//                logWarning(s"Loading $name class from ${l.getClassLoader.toString}")
//              } else if (l != null) {
//                logWarning(s"Loading $name class - CLASS LOADER NULL")
//              } else {
//                logWarning(s"Loading $name class - NOT FOUND")
//              }
//            }
            l
          }
          def doLoadClass(name: String, resolve: Boolean): Class[_] = {
            val classFileName = name.replaceAll("\\.", "/") + ".class"
            if (isBarrierClass(name)) {
              // For barrier classes, we construct a new copy of the class.
              val bytes = IOUtils.toByteArray(baseClassLoader.getResourceAsStream(classFileName))
              logDebug(s"custom defining: $name - ${java.util.Arrays.hashCode(bytes)}")
              defineClass(name, bytes, 0, bytes.length)
            } else if (!isSharedClass(name)) {
              logDebug(s"hive class: $name - ${getResource(classToPath(name))}")
              super.loadClass(name, resolve)
            } else {
              // For shared classes, we delegate to baseClassLoader, but fall back in case the
              // class is not found.
              logDebug(s"shared class: $name")
              try {
                baseClassLoader.loadClass(name)
              } catch {
                case _: ClassNotFoundException =>
                  super.loadClass(name, resolve)
              }
            }
          }
        }
      } else {
        baseClassLoader
      }
    // Right now, we create a URLClassLoader that gives preference to isolatedClassLoader
    // over its own URLs when it loads classes and resources.
    // We may want to use ChildFirstURLClassLoader based on
    // the configuration of spark.executor.userClassPathFirst, which gives preference
    // to its own URLs over the parent class loader (see Executor's createClassLoader method).
    new NonClosableMutableURLClassLoaderNew(isolatedClassLoader)
  }

  def addJar(path: URL): Unit = synchronized {
    classLoader.addURL(path)
  }

  /**
    * The place holder for shared Hive client for all the HiveContext sessions (they share an
    * IsolatedClientLoader).
    */
  var cachedHive: Any = null
}

class NonClosableMutableURLClassLoaderNew(parent: ClassLoader)
  extends MutableURLClassLoader(Array.empty, parent) {

  override def close(): Unit = {}
}