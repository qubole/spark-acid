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

import java.io.{ByteArrayInputStream, FileNotFoundException, IOException, ObjectInputStream}
import java.text.SimpleDateFormat
import java.util.{Locale, Properties}

import org.apache.hadoop.conf.{Configurable, Configuration}
import com.qubole.shaded.hive.ql.io.AcidUtils
import com.qubole.shaded.hive.ql.io.orc.{OrcSerde, OrcSplit}
import com.qubole.shaded.hive.serde2.objectinspector.{ObjectInspectorConverters, StructObjectInspector}
import com.qubole.shaded.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.lib.CombineFileSplit
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.TaskType
import org.apache.hadoop.util.ReflectionUtils
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import com.qubole.spark.util.InputFileBlockHolder
import org.apache.spark.{InterruptibleIterator, Partition, SerializableWritable, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SpecificInternalRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import com.qubole.spark.rdd.HiveRDD.{toCatalystDecimal, unwrapperFor}
import com.qubole.spark.util.{NextIterator, Util}
import org.apache.spark.unsafe.types.UTF8String

class HiveRddHelper(theSplit: Partition, jobConf: JobConf,
                    newInputFormatClassString: String, createTime: java.util.Date,
                    ignoreCorruptFilesStr: String, ignoreMissingFilesStr: String,
                    deserializerClassName: String, broadcastedConfiguration: Configuration,
                    tableProps: Properties, mutableRow: SpecificInternalRow,
                    attrsWithIndex: Seq[(Attribute, Int)],
                    context: TaskContext) extends Logging {

  val ignoreCorruptFiles = ignoreCorruptFilesStr == "true"
  val ignoreMissingFiles = ignoreMissingFilesStr == "true"

  def getInputFormat(newInputFormatClassString: String,
                     conf: JobConf): InputFormat[Writable, Writable] = {
    val newInputFormat = {
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

  def getInputSplit(bytes: Array[Byte]): SerializableWritable[InputSplit] = {
    val bis = new ByteArrayInputStream(bytes)
    var in: ObjectInputStream = null;
    try {
      in = new ObjectInputStream(bis);
      val o = in.readObject()
      o.asInstanceOf[SerializableWritable[InputSplit]]
    } finally {
      try {
        if (in != null) {
          in.close();
        }
      } catch {
        case e: IOException =>
          logError("exception", e)
      }
    }
  }



  def compute(): InterruptibleIterator[InternalRow] = {
      logInfo("------------------------------------ Checkpoint -3")
      val iter = new NextIterator[(Writable, Writable)] {
        private val split = theSplit.asInstanceOf[HivePartition]
        val inputSplit = getInputSplit(split.getInputSplitBytes()).value

        logInfo("Input split: " + inputSplit)
        val scheme = Util.getSplitScheme(inputSplit)

        val inputMetrics = context.taskMetrics().inputMetrics
        val existingBytesRead = inputMetrics.bytesRead
//        val blobStoreInputMetrics: Option[InputMetrics] = if (scheme == SplitFileSystemType.BLOB_STORE ) {
//          Some(context.taskMetrics().blobStoreInputMetrics)
//        } else {
//          None
//        }
        val blobStoreInputMetrics: Option[InputMetrics] = None
        val existingBlobStoreBytesRead = blobStoreInputMetrics.map(_.bytesRead).sum

        // Sets InputFileBlockHolder for the file block's information
        inputSplit match {
          case fs: FileSplit =>
            InputFileBlockHolder.set(fs.getPath.toString, fs.getStart, fs.getLength)
          case _ =>
            InputFileBlockHolder.unset()
        }

        // Find a function that will return the FileSystem bytes read by this thread. Do this before
        // creating RecordReader, because RecordReader's constructor might read some bytes
        private val getBytesReadCallback: Option[() => Long] = inputSplit match {
//          case _: FileSplit | _: CombineFileSplit =>
//            SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
          case _ => None
        }

        // We get our input bytes from thread-local Hadoop FileSystem statistics.
        // If we do a coalesce, however, we are likely to compute multiple partitions in the same
        // task and in the same thread, in which case we need to avoid override values written by
        // previous partitions (SPARK-13071).
        private def updateBytesRead(): Unit = {
//          getBytesReadCallback.foreach { getBytesRead =>
//            inputMetrics.setBytesRead(existingBytesRead + getBytesRead())
//            blobStoreInputMetrics.foreach(_.setBytesRead(existingBlobStoreBytesRead + getBytesRead()))
//          }
        }

        private var reader: RecordReader[Writable, Writable] = null
        private val inputFormat = getInputFormat(newInputFormatClassString,
          jobConf)
        addLocalConfiguration(
          new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(createTime),
          context.stageId, theSplit.index, context.attemptNumber, jobConf)

        val isFullAcidScan = AcidUtils.isFullAcidScan(jobConf)
        logInfo(s"isFullAcidScan= $isFullAcidScan")
        reader =
          try {
            val iSplit = inputSplit
            inputFormat.getRecordReader(iSplit, jobConf, Reporter.NULL)
          } catch {
            case e: FileNotFoundException if ignoreMissingFiles =>
              logWarning(s"Skipped missing file: ${inputSplit}", e)
              finished = true
              null
            // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
            case e: FileNotFoundException if !ignoreMissingFiles => throw e
            case e: IOException if ignoreCorruptFiles =>
              logWarning(s"Skipped the rest content in the corrupted file: ${inputSplit}", e)
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

        private val key: Writable = if (reader == null) {
          null.asInstanceOf[Writable]
        } else {
          reader.createKey()
        }
        private val value: Writable = if (reader == null) {
          null.asInstanceOf[Writable]
        } else {
          reader.createValue()
        }

        override def getNext(): (Writable, Writable) = {
          logInfo("------------------------------------ Checkpoint getNext")
          try {
            finished = !reader.next(key, value)
          } catch {
            case e: FileNotFoundException if ignoreMissingFiles =>
              logWarning(s"Skipped missing file: ${inputSplit}", e)
              finished = true
            // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
            case e: FileNotFoundException if !ignoreMissingFiles => throw e
            case e: IOException if ignoreCorruptFiles =>
              logWarning(s"Skipped the rest content in the corrupted file: ${inputSplit}", e)
              finished = true
          }
//          if (!finished) {
//            inputMetrics.incRecordsRead(1)
//            blobStoreInputMetrics.foreach(_.incRecordsRead(1))
//          }
//          if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
//            updateBytesRead()
//          }
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
            } else if (inputSplit.isInstanceOf[FileSplit] ||
              inputSplit.isInstanceOf[CombineFileSplit]) {
              // If we can't get the bytes read from the FS stats, fall back to the split size,
              // which may be inaccurate.
//              try {
//                inputMetrics.incBytesRead(inputSplit.getLength)
//                blobStoreInputMetrics.foreach(_.incBytesRead(inputSplit.getLength))
//              } catch {
//                case e: java.io.IOException =>
//                  logWarning("Unable to get input size to set InputMetrics for task", e)
//              }
            }
          }
        }
      }
      logInfo("------------------------------------ Checkpoint 1")
      import com.qubole.shaded.hive.serde2.Deserializer
      logInfo("------------------------------------ Checkpoint 2")

      val deserializerClassNew = Util.classForName(
        deserializerClassName)
      val deserializer = deserializerClassNew.newInstance().asInstanceOf[Deserializer]
      deserializer.initialize(broadcastedConfiguration, tableProps)
      logInfo("------------------------------------ Checkpoint 4")
      val rawDeser = deserializer
      val tableDeser = deserializer
      val iterator = iter.map(_._2)

      logInfo("------------------------------------ Checkpoint 5")
      def fillObject(nonPartitionKeyAttrs: Seq[(Attribute, Int)],
                     mutableRow: InternalRow): Iterator[InternalRow] = {

        val soi = if (rawDeser.getObjectInspector.equals(tableDeser.getObjectInspector)) {
          rawDeser.getObjectInspector.asInstanceOf[StructObjectInspector]
        } else {
          ObjectInspectorConverters.getConvertedOI(
            rawDeser.getObjectInspector,
            tableDeser.getObjectInspector).asInstanceOf[StructObjectInspector]
        }

        logDebug(soi.toString)

        val (fieldRefs, fieldOrdinals) = nonPartitionKeyAttrs.map { case (attr, ordinal) =>
          soi.getStructFieldRef(attr.name) -> ordinal
        }.toArray.unzip

        /**
          * Builds specific unwrappers ahead of time according to object inspector
          * types to avoid pattern matching and branching costs per row.
          */
        val unwrappers: Seq[(Any, InternalRow, Int) => Unit] = fieldRefs.map {
          x =>
          val y = x.getFieldObjectInspector
          y match {
            case oi: BooleanObjectInspector =>
              (value: Any, row: InternalRow, ordinal: Int) => row.setBoolean(ordinal, oi.get(value))
            case oi: ByteObjectInspector =>
              (value: Any, row: InternalRow, ordinal: Int) => row.setByte(ordinal, oi.get(value))
            case oi: ShortObjectInspector =>
              (value: Any, row: InternalRow, ordinal: Int) => row.setShort(ordinal, oi.get(value))
            case oi: IntObjectInspector =>
              (value: Any, row: InternalRow, ordinal: Int) => row.setInt(ordinal, oi.get(value))
            case oi: LongObjectInspector =>
              (value: Any, row: InternalRow, ordinal: Int) => row.setLong(ordinal, oi.get(value))
            case oi: FloatObjectInspector =>
              (value: Any, row: InternalRow, ordinal: Int) => row.setFloat(ordinal, oi.get(value))
            case oi: DoubleObjectInspector =>
              (value: Any, row: InternalRow, ordinal: Int) => row.setDouble(ordinal, oi.get(value))
            case oi: HiveVarcharObjectInspector =>
              (value: Any, row: InternalRow, ordinal: Int) =>
                row.update(ordinal, UTF8String.fromString(oi.getPrimitiveJavaObject(value).getValue))
            case oi: HiveCharObjectInspector =>
              (value: Any, row: InternalRow, ordinal: Int) =>
                row.update(ordinal, UTF8String.fromString(oi.getPrimitiveJavaObject(value).getValue))
            case oi: HiveDecimalObjectInspector =>
              (value: Any, row: InternalRow, ordinal: Int) =>
                row.update(ordinal, toCatalystDecimal(oi, value))
            case oi: TimestampObjectInspector =>
              (value: Any, row: InternalRow, ordinal: Int) =>
                row.setLong(ordinal, DateTimeUtils.fromJavaTimestamp(
                  oi.getPrimitiveJavaObject(value).toSqlTimestamp))
            case oi: DateObjectInspector =>
              (value: Any, row: InternalRow, ordinal: Int) =>
                val y = oi.getPrimitiveWritableObject(value).get().toEpochMilli
                row.setInt(ordinal, DateTimeUtils.fromJavaDate(new java.sql.Date(y)))
            case oi: BinaryObjectInspector =>
              (value: Any, row: InternalRow, ordinal: Int) =>
                row.update(ordinal, oi.getPrimitiveJavaObject(value))
            case oi =>
              val unwrapper = unwrapperFor(oi)
              (value: Any, row: InternalRow, ordinal: Int) => row(ordinal) = unwrapper(value)
          }
        }

        val converter = ObjectInspectorConverters.getConverter(rawDeser.getObjectInspector, soi)

        // Map each tuple to a row object
        iterator.map { value =>
          val raw = converter.convert(rawDeser.deserialize(value))
          var i = 0
          val length = fieldRefs.length
          while (i < length) {
            val fieldValue = soi.getStructFieldData(raw, fieldRefs(i))
            if (fieldValue == null) {
              mutableRow.setNullAt(fieldOrdinals(i))
            } else {
              unwrappers(i)(fieldValue, mutableRow, fieldOrdinals(i))
            }
            i += 1
          }

          mutableRow: InternalRow
        }
      }

      logInfo("------------------------------------ Checkpoint 6")
      val newIter = fillObject(attrsWithIndex, mutableRow)
      logInfo("------------------------------------ Checkpoint 7")
      new InterruptibleIterator[InternalRow](context, newIter)
    }
}
