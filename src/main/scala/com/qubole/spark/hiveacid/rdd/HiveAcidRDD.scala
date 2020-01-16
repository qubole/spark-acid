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

package com.qubole.spark.hiveacid.rdd

import java.io.{FileNotFoundException, IOException}
import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentHashMap
import java.util.{Date, Locale}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import com.qubole.shaded.hadoop.hive.common.ValidWriteIdList
import com.qubole.shaded.hadoop.hive.ql.io.{AcidInputFormat, AcidUtils, HiveInputFormat, RecordIdentifier}
import com.qubole.spark.hiveacid.rdd.HiveAcidRDD.HiveAcidPartitionsWithSplitRDD
import com.qubole.spark.hiveacid.util.{SerializableConfiguration, Util}
import com.qubole.spark.hiveacid.util.{SerializableWritable => _}
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileInputFormat, _}
import org.apache.hadoop.mapreduce.TaskType
import org.apache.hadoop.util.ReflectionUtils
import org.apache.spark.{Partitioner, _}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

// This file has lot of borrowed code from org.apache.spark.rdd.HadoopRdd

private object Cache {
  val jobConf =  new ConcurrentHashMap[String, Any]()
}

private class HiveAcidPartition(rddId: Int, override val index: Int, s: InputSplit)
  extends Partition {

  val inputSplit = new SerializableWritable[InputSplit](s)

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}

/**
 * An RDD that provides core functionality for reading data stored in Hadoop (e.g., files in HDFS,
 * sources in HBase, or S3), using the older MapReduce API (`org.apache.hadoop.mapred`).
 *
 * @param sc The SparkContext to associate the RDD with.
 * @param validWriteIds The list of valid write ids.
 * @param isFullAcidTable if table is full acid table.
 * @param broadcastedConf A general Hadoop Configuration, or a subclass of it. If the enclosed
 *   variable references an instance of JobConf, then that JobConf will be used for the Hadoop job.
 *   Otherwise, a new JobConf will be created on each slave using the enclosed Configuration.
 * @param initLocalJobConfFuncOpt Optional closure used to initialize any JobConf that HiveAcidRDD
 *     creates.
 * @param inputFormatClass Storage format of the data to be read.
 * @param keyClass Class of the key associated with the inputFormatClass.
 * @param valueClass Class of the value associated with the inputFormatClass.
 * @param minPartitions Minimum number of HiveAcidRDD partitions (Hadoop Splits) to generate.
 *
 * @note Instantiating this class directly is not recommended, please use
 * `org.apache.spark.SparkContext.HiveAcidRDD()`
 */
private[hiveacid] class HiveAcidRDD[K, V](sc: SparkContext,
                                     @transient val validWriteIds: ValidWriteIdList,
                                     @transient val isFullAcidTable: Boolean,
                                     broadcastedConf: Broadcast[SerializableConfiguration],
                                     initLocalJobConfFuncOpt: Option[JobConf => Unit],
                                     inputFormatClass: Class[_ <: InputFormat[K, V]],
                                     keyClass: Class[K],
                                     valueClass: Class[V],
                                     minPartitions: Int)
  extends RDD[(RecordIdentifier, V)](sc, Nil) with Logging {

  def this(
            sc: SparkContext,
            @transient validWriteIds: ValidWriteIdList,
            @transient isFullAcidTable: Boolean,
            conf: JobConf,
            inputFormatClass: Class[_ <: InputFormat[K, V]],
            keyClass: Class[K],
            valueClass: Class[V],
            minPartitions: Int) = {
    this(
      sc,
      validWriteIds,
      isFullAcidTable,
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
    sparkContext.getConf.getBoolean("spark.hadoop.cloneConf", defaultValue = false)

  private val ignoreCorruptFiles =
    sparkContext.getConf.getBoolean("spark.files.ignoreCorruptFiles", defaultValue = false)

  private val ignoreMissingFiles =
    sparkContext.getConf.getBoolean("spark.files.ignoreMissingFiles", defaultValue = false)

  private val ignoreEmptySplits =
    sparkContext.getConf.getBoolean("spark.hadoopRDD.ignoreEmptySplits", defaultValue = false)

  // Returns a JobConf that will be used on slaves to obtain input splits for Hadoop reads.
  protected def getJobConf: JobConf = {
    val conf: Configuration = broadcastedConf.value.value
    if (shouldCloneJobConf) {
      // Hadoop Configuration objects are not thread-safe, which may lead to various problems if
      // one job modifies a configuration while another reads it (SPARK-2546).  This problem occurs
      // somewhat rarely because most jobs treat the configuration as though it's immutable.  One
      // solution, implemented here, is to clone the Configuration object.  Unfortunately, this
      // clone can be very expensive.  To avoid unexpected performance regressions for workloads and
      // Hadoop versions that do not suffer from these thread-safety issues, this cloning is
      // disabled by default.
      HiveAcidRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
        logDebug("Cloning Hadoop Configuration")
        val newJobConf = new JobConf(conf)
        if (!conf.isInstanceOf[JobConf]) {
          initLocalJobConfFuncOpt.foreach(f => f(newJobConf))
        }
        newJobConf
      }
    } else {
      conf match {
        case c: JobConf =>
          logDebug("Re-using user-broadcasted JobConf")
          c
        case _ =>
          Option(HiveAcidRDD.getCachedMetadata(jobConfCacheKey))
            .map { conf =>
              logDebug("Re-using cached JobConf")
              conf.asInstanceOf[JobConf]
            }
            .getOrElse {
              // Create a JobConf that will be cached and used across this RDD's getJobConf() calls in
              // the local process. The local cache is accessed through HiveAcidRDD.putCachedMetadata().
              // The caching helps minimize GC, since a JobConf can contain ~10KB of temporary
              // objects. Synchronize to prevent ConcurrentModificationException (SPARK-1097,
              // HADOOP-10456).
              HiveAcidRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
                logDebug("Creating new JobConf and caching it for later re-use")
                val newJobConf = new JobConf(conf)
                initLocalJobConfFuncOpt.foreach(f => f(newJobConf))
                HiveAcidRDD.putCachedMetadata(jobConfCacheKey, newJobConf)
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
    var jobConf = getJobConf

    if (isFullAcidTable) {
      // If full ACID table, just set the right writeIds, the
      // OrcInputFormat.getSplits() will take care of the rest
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
        array(i) = new HiveAcidPartition(id, i, inputSplits(i))
      }
      array
    } catch {
      case e: InvalidInputException if ignoreMissingFiles =>
        val inputDir = jobConf.get(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR)
        logWarning(s"$inputDir doesn't exist and no" +
          s" partitions returned from this path.", e)
        Array.empty[Partition]
    }
  }

  override def compute(theSplit: Partition,
                       context: TaskContext): InterruptibleIterator[(RecordIdentifier, V)] = {
    val iter: NextIterator[(RecordIdentifier, V)] = new NextIterator[(RecordIdentifier, V)] {

      private val split = theSplit.asInstanceOf[HiveAcidPartition]
      logDebug("Input split: " + split.inputSplit)
      val jobConf: JobConf = getJobConf

      private var reader: RecordReader[K, V] = _
      private val inputFormat = getInputFormat(jobConf)
      HiveAcidRDD.addLocalConfiguration(
        new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(createTime),
        context.stageId, theSplit.index, context.attemptNumber, jobConf)

      reader =
        try {
          // Underlying code is not MT safe. Synchronize
          // while creating record reader
          HiveAcidRDD.RECORD_READER_INIT_LOCK.synchronized {
            inputFormat.getRecordReader(split.inputSplit.value, jobConf, Reporter.NULL)
          }
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
      context.addTaskCompletionListener[Unit] { _ =>
        closeIfNeeded()
      }

      private val key: K = if (reader == null) null.asInstanceOf[K] else reader.createKey()
      private val value: V = if (reader == null) null.asInstanceOf[V] else reader.createValue()
      private var recordIdentifier: RecordIdentifier = _
      private val acidRecordReader = reader match {
        case acidReader: AcidInputFormat.AcidRecordReader[_, _] =>
          acidReader
        case _ =>
          null
      }

      override def getNext(): (RecordIdentifier, V) = {
        try {
          finished = !reader.next(key, value)
          if (!finished && acidRecordReader != null) {
            recordIdentifier = acidRecordReader.getRecordIdentifier
          }
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
        (recordIdentifier, value)
      }

      override def close(): Unit = {
        if (reader != null) {
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
        }
      }
    }
    new InterruptibleIterator[(RecordIdentifier, V)](context, iter)
  }



  /** Maps over a partition, providing the InputSplit that was used as the base of the partition. */
  @DeveloperApi
  def mapPartitionsWithInputSplit[U: ClassTag](
      f: (InputSplit, Iterator[(RecordIdentifier, V)]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = {
    new HiveAcidPartitionsWithSplitRDD(this, f, preservesPartitioning)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val hsplit = split.asInstanceOf[HiveAcidPartition].inputSplit.value
    val locs = hsplit match {
      case lsplit: InputSplitWithLocationInfo =>
        HiveAcidRDD.convertSplitLocationInfo(lsplit.getLocationInfo)
      case _ => None
    }
    locs.getOrElse(hsplit.getLocations.filter(_ != "localhost"))
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  override def persist(storageLevel: StorageLevel): this.type = {
    if (storageLevel.deserialized) {
      logWarning("Caching HiveAcidRDDs as deserialized objects usually leads to undesired" +
        " behavior because Hadoop's RecordReader reuses the same Writable object for all records." +
        " Use a map transformation to make copies of the records.")
    }
    super.persist(storageLevel)
  }

  def getConf: Configuration = getJobConf
}

object HiveAcidRDD extends Logging {

  /*
   * Use of utf8Decoder inside OrcRecordUpdater is not MT safe when peforming
   * getRecordReader. This leads to illlegal state exeception when called in
   * parallel by multiple tasks in single executor (JVM). Synchronize !!
   */
  val RECORD_READER_INIT_LOCK = new Object()

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
  class HiveAcidPartitionsWithSplitRDD[U: ClassTag, T: ClassTag](
                                                               prev: RDD[T],
                                                               f: (InputSplit, Iterator[T]) => Iterator[U],
                                                               preservesPartitioning: Boolean = false)
    extends RDD[U](prev) {

    override val partitioner: Option[Partitioner] = if (preservesPartitioning) firstParent[T].partitioner else None

    override def getPartitions: Array[Partition] = firstParent[T].partitions

    override def compute(split: Partition, context: TaskContext): Iterator[U] = {
      val partition = split.asInstanceOf[HiveAcidPartition]
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

/**
 * Borrowed from org.apache.spark.util.NextIterator
 */
private abstract class NextIterator[U] extends Iterator[U] {

  private var gotNext = false
  private var nextValue: U = _
  private var closed = false
  protected var finished = false

  /**
    * Method for subclasses to implement to provide the next element.
    *
    * If no next element is available, the subclass should set `finished`
    * to `true` and may return any value (it will be ignored).
    *
    * This convention is required because `null` may be a valid value,
    * and using `Option` seems like it might create unnecessary Some/None
    * instances, given some iterators might be called in a tight loop.
    *
    * @return U, or set 'finished' when done
    */
  protected def getNext(): U

  /**
    * Method for subclasses to implement when all elements have been successfully
    * iterated, and the iteration is done.
    *
    * <b>Note:</b> `NextIterator` cannot guarantee that `close` will be
    * called because it has no control over what happens when an exception
    * happens in the user code that is calling hasNext/next.
    *
    * Ideally you should have another try/catch, as in HadoopRDD, that
    * ensures any resources are closed should iteration fail.
    */
  protected def close()

  /**
    * Calls the subclass-defined close method, but only once.
    *
    * Usually calling `close` multiple times should be fine, but historically
    * there have been issues with some InputFormats throwing exceptions.
    */
  def closeIfNeeded() {
    if (!closed) {
      // Note: it's important that we set closed = true before calling close(), since setting it
      // afterwards would permit us to call close() multiple times if close() threw an exception.
      closed = true
      close()
    }
  }

  override def hasNext: Boolean = {
    if (!finished) {
      if (!gotNext) {
        nextValue = getNext()
        if (finished) {
          closeIfNeeded()
        }
        gotNext = true
      }
    }
    !finished
  }

  override def next(): U = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    gotNext = false
    nextValue
  }
}
