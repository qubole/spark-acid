/*
 * Copyright 2019 Qubole, Inc.  All rights reserved.
 *
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

package com.qubole.spark.datasources.hiveacid.rdd

import java.io.{FileNotFoundException, IOException}
import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentHashMap
import java.util.{Date, Locale}

import com.qubole.shaded.hadoop.hive.common.ValidWriteIdList
import com.qubole.shaded.hadoop.hive.ql.io.{AcidUtils, HiveInputFormat}
import com.qubole.spark.datasources.hiveacid.HiveAcidState
import com.qubole.spark.datasources.hiveacid.util.{InputFileBlockHolder, NextIterator, SerializableConfiguration, Util}
import com.qubole.spark.datasources.hiveacid.rdd.Hive3RDD.Hive3PartitionsWithSplitRDD
import com.qubole.spark.datasources.hiveacid.util.{SerializableWritable => _, _}
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.lib.CombineFileSplit
import org.apache.hadoop.mapred.{FileInputFormat, _}
import org.apache.hadoop.mapreduce.TaskType
import org.apache.hadoop.util.ReflectionUtils
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object Cache {
  import com.google.common.collect.MapMaker
  val jobConf =  new ConcurrentHashMap[String, Any]()
}

class Hive3Partition(rddId: Int, override val index: Int, s: InputSplit)
  extends Partition {

  val inputSplit = new SerializableWritable[InputSplit](s)

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}

/**
  * :: DeveloperApi ::
  * An RDD that provides core functionality for reading data stored in Hadoop (e.g., files in HDFS,
  * sources in HBase, or S3), using the older MapReduce API (`org.apache.hadoop.mapred`).
  *
  * @param sc The SparkContext to associate the RDD with.
  * @param broadcastedConf A general Hadoop Configuration, or a subclass of it. If the enclosed
  *   variable references an instance of JobConf, then that JobConf will be used for the Hadoop job.
  *   Otherwise, a new JobConf will be created on each slave using the enclosed Configuration.
  * @param initLocalJobConfFuncOpt Optional closure used to initialize any JobConf that Hive3RDD
  *     creates.
  * @param inputFormatClass Storage format of the data to be read.
  * @param keyClass Class of the key associated with the inputFormatClass.
  * @param valueClass Class of the value associated with the inputFormatClass.
  * @param minPartitions Minimum number of Hive3RDD partitions (Hadoop Splits) to generate.
  *
  * @note Instantiating this class directly is not recommended, please use
  * `org.apache.spark.SparkContext.Hive3RDD()`
  */
@DeveloperApi
class Hive3RDD[K, V](
                      sc: SparkContext,
                      @transient val acidState: HiveAcidState,
                      broadcastedConf: Broadcast[SerializableConfiguration],
                      initLocalJobConfFuncOpt: Option[JobConf => Unit],
                      inputFormatClass: Class[_ <: InputFormat[K, V]],
                      keyClass: Class[K],
                      valueClass: Class[V],
                      minPartitions: Int)
  extends RDD[(K, V)](sc, Nil) with Logging {

  def this(
            sc: SparkContext,
            @transient acidState: HiveAcidState,
            conf: JobConf,
            inputFormatClass: Class[_ <: InputFormat[K, V]],
            keyClass: Class[K],
            valueClass: Class[V],
            minPartitions: Int) = {
    this(
      sc,
      acidState,
      sc.broadcast(new SerializableConfiguration(conf))
        .asInstanceOf[Broadcast[SerializableConfiguration]],
      initLocalJobConfFuncOpt = None,
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions)
  }

  protected val jobConfCacheKey: String = "rdd_%d_job_conf".format(id)

  protected val inputFormatCacheKey: String = "rdd_%d_input_format".format(id)

  // used to build JobTracker ID
  private val createTime = new Date()

  private val shouldCloneJobConf =
    sparkContext.getConf.getBoolean("spark.hadoop.cloneConf", false)

  private val ignoreCorruptFiles =
    sparkContext.getConf.getBoolean("spark.files.ignoreCorruptFiles", false)

  private val ignoreMissingFiles =
    sparkContext.getConf.getBoolean("spark.files.ignoreMissingFiles", false)

  private val ignoreEmptySplits =
    sparkContext.getConf.getBoolean("spark.hadoopRDD.ignoreEmptySplits", false)

  // Returns a JobConf that will be used on slaves to obtain input splits for Hadoop reads.
  protected def getJobConf(): JobConf = {
    val conf: Configuration = broadcastedConf.value.value
    if (shouldCloneJobConf) {
      // Hadoop Configuration objects are not thread-safe, which may lead to various problems if
      // one job modifies a configuration while another reads it (SPARK-2546).  This problem occurs
      // somewhat rarely because most jobs treat the configuration as though it's immutable.  One
      // solution, implemented here, is to clone the Configuration object.  Unfortunately, this
      // clone can be very expensive.  To avoid unexpected performance regressions for workloads and
      // Hadoop versions that do not suffer from these thread-safety issues, this cloning is
      // disabled by default.
      Hive3RDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
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
        Option(Hive3RDD.getCachedMetadata(jobConfCacheKey))
          .map { conf =>
            logDebug("Re-using cached JobConf")
            conf.asInstanceOf[JobConf]
          }
          .getOrElse {
            // Create a JobConf that will be cached and used across this RDD's getJobConf() calls in
            // the local process. The local cache is accessed through Hive3RDD.putCachedMetadata().
            // The caching helps minimize GC, since a JobConf can contain ~10KB of temporary
            // objects. Synchronize to prevent ConcurrentModificationException (SPARK-1097,
            // HADOOP-10456).
            Hive3RDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
              logDebug("Creating new JobConf and caching it for later re-use")
              val newJobConf = new JobConf(conf)
              initLocalJobConfFuncOpt.foreach(f => f(newJobConf))
              Hive3RDD.putCachedMetadata(jobConfCacheKey, newJobConf)
              newJobConf
            }
          }
      }
    }
  }

  protected def getInputFormat(conf: JobConf): InputFormat[K, V] = {
    val newInputFormat = ReflectionUtils.newInstance(inputFormatClass.asInstanceOf[Class[_]], conf)
      .asInstanceOf[InputFormat[K, V]]
    newInputFormat match {
      case c: Configurable => c.setConf(conf)
      case _ =>
    }
    newInputFormat
  }

  override def getPartitions: Array[Partition] = {
    val validWriteIds: ValidWriteIdList = acidState.getValidWriteIds
    //val ValidWriteIdList = acidState.getValidWriteIdsNoTxn
    var jobConf = getJobConf()

    if (acidState.isFullAcidTable) {
      // If full ACID table, just set the right writeIds, the OrcInputFormat.getSplits() will take care of the rest
      AcidUtils.setValidWriteIdList(jobConf, validWriteIds)
    } else {
      val finalPaths = new ListBuffer[Path]()
      val pathsWithFileOriginals = new ListBuffer[Path]()
      val dirs = FileInputFormat.getInputPaths(jobConf).toSeq // Seq(acidState.location)
      HiveInputFormat.processPathsForMmRead(dirs, jobConf, validWriteIds,
        finalPaths, pathsWithFileOriginals)

      if (finalPaths.nonEmpty) {
        FileInputFormat.setInputPaths(jobConf, finalPaths.toList: _*)
        // Need recursive to be set to true because MM Tables can have a directory structure like:
        // ~/warehouse/hello_mm/base_0000034/HIVE_UNION_SUBDIR_1/000000_0
        // ~/warehouse/hello_mm/base_0000034/HIVE_UNION_SUBDIR_2/000000_0
        // ~/warehouse/hello_mm/delta_0000033_0000033_0001/HIVE_UNION_SUBDIR_1/000000_0
        // ~/warehouse/hello_mm/delta_0000033_0000033_0002/HIVE_UNION_SUBDIR_2/000000_0
        // ... which is created on UNION ALL operations
        jobConf.setBoolean(FileInputFormat.INPUT_DIR_RECURSIVE, true)
      }

      if (pathsWithFileOriginals.nonEmpty) {
        // We are going to add splits for these directories with recursive = false, so we ignore
        // any subdirectories (deltas or original directories) and only read the original files.
        // The fact that there's a loop calling addSplitsForGroup already implies it's ok to
        // the real input format multiple times... however some split concurrency/etc configs
        // that are applied separately in each call will effectively be ignored for such splits.
        jobConf = HiveInputFormat.createConfForMmOriginalsSplit(jobConf, pathsWithFileOriginals)
      }

    }
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    try {
      val allInputSplits = getInputFormat(jobConf).getSplits(jobConf, minPartitions)
      val inputSplits = if (ignoreEmptySplits) {
        allInputSplits.filter(_.getLength > 0)
      } else {
        allInputSplits
      }
      val array = new Array[Partition](inputSplits.size)
      for (i <- 0 until inputSplits.size) {
        array(i) = new Hive3Partition(id, i, inputSplits(i))
      }
      array
    } catch {
      case e: InvalidInputException if ignoreMissingFiles =>
        logWarning(s"${jobConf.get(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR)} doesn't exist and no" +
          s" partitions returned from this path.", e)
        Array.empty[Partition]
    }
  }

  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    val iter: NextIterator[(K, V)] = new NextIterator[(K, V)] {

      private val split = theSplit.asInstanceOf[Hive3Partition]
      logInfo("Input split: " + split.inputSplit)
      val scheme = Util.getSplitScheme(split.inputSplit.value)
      val jobConf = getJobConf()

      val inputMetrics = context.taskMetrics().inputMetrics
      val existingBytesRead = inputMetrics.bytesRead
      val blobStoreInputMetrics: Option[InputMetrics] = None
      val existingBlobStoreBytesRead = blobStoreInputMetrics.map(_.bytesRead).sum

      // Sets InputFileBlockHolder for the file block's information
      split.inputSplit.value match {
        case fs: FileSplit =>
          InputFileBlockHolder.set(fs.getPath.toString, fs.getStart, fs.getLength)
        case _ =>
          InputFileBlockHolder.unset()
      }

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      private val getBytesReadCallback: Option[() => Long] = None

      // We get our input bytes from thread-local Hadoop FileSystem statistics.
      // If we do a coalesce, however, we are likely to compute multiple partitions in the same
      // task and in the same thread, in which case we need to avoid override values written by
      // previous partitions (SPARK-13071).
      private def updateBytesRead(): Unit = {
        getBytesReadCallback.foreach { getBytesRead =>
//          inputMetrics.setBytesRead(existingBytesRead + getBytesRead())
//          blobStoreInputMetrics.foreach(_.setBytesRead(existingBlobStoreBytesRead + getBytesRead()))
        }
      }

      private var reader: RecordReader[K, V] = null
      private val inputFormat = getInputFormat(jobConf)
      Hive3RDD.addLocalConfiguration(
        new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(createTime),
        context.stageId, theSplit.index, context.attemptNumber, jobConf)

      reader =
        try {
          inputFormat.getRecordReader(split.inputSplit.value, jobConf, Reporter.NULL)
        } catch {
          case e: FileNotFoundException if ignoreMissingFiles =>
            logWarning(s"Skipped missing file: ${split.inputSplit}", e)
            finished = true
            null
          // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
          case e: FileNotFoundException if !ignoreMissingFiles => throw e
          case e: IOException if ignoreCorruptFiles =>
            logWarning(s"Skipped the rest content in the corrupted file: ${split.inputSplit}", e)
            finished = true
            null
        }
      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener[Unit] { context =>
        // Update the bytes read before closing is to make sure lingering bytesRead statistics in
        // this thread get correctly added.
        updateBytesRead()
        closeIfNeeded()
      }

      private val key: K = if (reader == null) null.asInstanceOf[K] else reader.createKey()
      private val value: V = if (reader == null) null.asInstanceOf[V] else reader.createValue()

      override def getNext(): (K, V) = {
        try {
          finished = !reader.next(key, value)
        } catch {
          case e: FileNotFoundException if ignoreMissingFiles =>
            logWarning(s"Skipped missing file: ${split.inputSplit}", e)
            finished = true
          // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
          case e: FileNotFoundException if !ignoreMissingFiles => throw e
          case e: IOException if ignoreCorruptFiles =>
            logWarning(s"Skipped the rest content in the corrupted file: ${split.inputSplit}", e)
            finished = true
        }
        if (!finished) {
//          inputMetrics.incRecordsRead(1)
//          blobStoreInputMetrics.foreach(_.incRecordsRead(1))
        }
//        if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
//          updateBytesRead()
//        }
        (key, value)
      }

      override def close(): Unit = {
        if (reader != null) {
          InputFileBlockHolder.unset()
          try {
            reader.close()
          } catch {
            case e: Exception =>
              if (!Util.inShutdown()) {
                logWarning("Exception in RecordReader.close()", e)
              }
          } finally {
            reader = null
          }
          if (getBytesReadCallback.isDefined) {
            updateBytesRead()
          } else if (split.inputSplit.value.isInstanceOf[FileSplit] ||
            split.inputSplit.value.isInstanceOf[CombineFileSplit]) {
            // If we can't get the bytes read from the FS stats, fall back to the split size,
            // which may be inaccurate.
            try {
//              inputMetrics.incBytesRead(split.inputSplit.value.getLength)
//              blobStoreInputMetrics.foreach(_.incBytesRead(split.inputSplit.value.getLength))
            } catch {
              case e: java.io.IOException =>
                logWarning("Unable to get input size to set InputMetrics for task", e)
            }
          }
        }
      }
    }
    new InterruptibleIterator[(K, V)](context, iter)
  }



  /** Maps over a partition, providing the InputSplit that was used as the base of the partition. */
  @DeveloperApi
  def mapPartitionsWithInputSplit[U: ClassTag](
                                                f: (InputSplit, Iterator[(K, V)]) => Iterator[U],
                                                preservesPartitioning: Boolean = false): RDD[U] = {
    new Hive3PartitionsWithSplitRDD(this, f, preservesPartitioning)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val hsplit = split.asInstanceOf[Hive3Partition].inputSplit.value
    val locs = hsplit match {
      case lsplit: InputSplitWithLocationInfo =>
        Hive3RDD.convertSplitLocationInfo(lsplit.getLocationInfo)
      case _ => None
    }
    locs.getOrElse(hsplit.getLocations.filter(_ != "localhost"))
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  override def persist(storageLevel: StorageLevel): this.type = {
    if (storageLevel.deserialized) {
      logWarning("Caching Hive3RDDs as deserialized objects usually leads to undesired" +
        " behavior because Hadoop's RecordReader reuses the same Writable object for all records." +
        " Use a map transformation to make copies of the records.")
    }
    super.persist(storageLevel)
  }

  def getConf: Configuration = getJobConf()
}

object Hive3RDD extends Logging {
  /**
    * Configuration's constructor is not threadsafe (see SPARK-1097 and HADOOP-10456).
    * Therefore, we synchronize on this lock before calling new JobConf() or new Configuration().
    */
  val CONFIGURATION_INSTANTIATION_LOCK = new Object()

  /** Update the input bytes read metric each time this number of records has been read */
  val RECORDS_BETWEEN_BYTES_READ_METRIC_UPDATES = 256

  def getCachedMetadata(key: String): Any = {
    Cache.jobConf.get(key)
  }

  private def putCachedMetadata(key: String, value: Any): Unit = {
    Cache.jobConf.put(key, value)
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

  /**
    * Analogous to [[org.apache.spark.rdd.MapPartitionsRDD]], but passes in an InputSplit to
    * the given function rather than the index of the partition.
    */
  class Hive3PartitionsWithSplitRDD[U: ClassTag, T: ClassTag](
                                                               prev: RDD[T],
                                                               f: (InputSplit, Iterator[T]) => Iterator[U],
                                                               preservesPartitioning: Boolean = false)
    extends RDD[U](prev) {

    override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

    override def getPartitions: Array[Partition] = firstParent[T].partitions

    override def compute(split: Partition, context: TaskContext): Iterator[U] = {
      val partition = split.asInstanceOf[Hive3Partition]
      val inputSplit = partition.inputSplit.value
      f(inputSplit, firstParent[T].iterator(split, context))
    }
  }

  def convertSplitLocationInfo(
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
