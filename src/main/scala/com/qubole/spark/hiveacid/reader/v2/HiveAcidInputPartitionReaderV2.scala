package com.qubole.spark.hiveacid.reader.v2

import java.io.IOException
import java.util._
import java.util.List

import scala.collection.JavaConverters._
import com.qubole.spark.hiveacid.util.SerializableConfiguration
import com.qubole.spark.hiveacid.rdd.HiveAcidPartition
import org.apache.hadoop.mapred.JobConf
import com.qubole.shaded.orc.TypeDescription
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.TaskContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapred.TaskID
import org.apache.hadoop.mapreduce.TaskType
import org.apache.hadoop.mapred.JobID
import org.apache.spark.sql.execution.datasources.orc._
import com.qubole.shaded.orc.OrcFile
import com.qubole.shaded.orc.OrcConf
import com.qubole.shaded.hadoop.hive.ql.io.orc.OrcSplit
import com.qubole.shaded.hadoop.hive.serde2.ColumnProjectionUtils

private[v2] class HiveAcidInputPartitionReaderV2(split: HiveAcidPartition,
                                                 broadcastedConf: Broadcast[SerializableConfiguration],
                                                 partitionValues : InternalRow,
                                                 requiredFields: Array[StructField],
                                                 partitionSchema : StructType,
                                                 isFullAcidTable: Boolean)
        extends InputPartitionReader[ColumnarBatch] {
  //TODO : Need to get a unique id to cache the jobConf.
  private val jobConf = new JobConf(broadcastedConf.value.value)
  private val orcColumnarBatchReader = new OrcColumnarBatchReader(1024)

  private def initReader() : Unit = {
    // Get the reader schema using the column names and types set in hive conf.
    val readerSchema: TypeDescription =
      com.qubole.shaded.hadoop.hive.ql.io.orc.OrcInputFormat.getDesiredRowTypeDescr(jobConf, true, 2147483647)

    // Set it as orc.mapred.input.schema so that the reader will read only the required columns
    jobConf.set("orc.mapred.input.schema", readerSchema.toString)

    val fileSplit = split.inputSplit.value.asInstanceOf[OrcSplit]
    val readerLocal = OrcFile.createReader(fileSplit.getPath,
      OrcFile.readerOptions(jobConf).maxLength(
        OrcConf.MAX_FILE_LENGTH.getLong(jobConf)).filesystem(fileSplit.getPath.getFileSystem(jobConf)))

    // Get the column id from hive conf para. TODO : Can be sent via a parameter
    val colIds = jobConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR)
    val requestedColIds = if (!colIds.isEmpty()) {
      colIds.split(",").map(a => a.toInt)
    } else {
      Array[Int]()
    }

    // Register the listener for closing the reader before init is done.
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val taskAttemptContext = new org.apache.hadoop.mapred.TaskAttemptContextImpl(jobConf, attemptId)
    val iter = new org.apache.spark.sql.execution.datasources.RecordReaderIterator(orcColumnarBatchReader)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))

    //TODO: Need to generalize it for supporting other kind of file format.
    orcColumnarBatchReader.initialize(fileSplit, taskAttemptContext)
    orcColumnarBatchReader.initBatch(readerLocal.getSchema, requestedColIds,
      requiredFields, partitionSchema, partitionValues, isFullAcidTable && !fileSplit.isOriginal)
  }
  initReader()

  @throws(classOf[IOException])
  override def next() : Boolean = {
    orcColumnarBatchReader.nextKeyValue()
  }

  override def get () : ColumnarBatch = {
    orcColumnarBatchReader.getCurrentValue
  }

  @throws(classOf[IOException])
  override def close() : Unit = {
    orcColumnarBatchReader.close()
  }
}
