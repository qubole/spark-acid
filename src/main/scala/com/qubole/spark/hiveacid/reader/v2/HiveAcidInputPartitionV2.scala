package com.qubole.spark.hiveacid.reader.v2

import com.qubole.spark.hiveacid.rdd.HiveAcidPartition
import com.qubole.spark.hiveacid.util.{SerializableConfiguration}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

private[hiveacid] class HiveAcidInputPartitionV2(split: HiveAcidPartition,
                                                 broadcastedConf: Broadcast[SerializableConfiguration],
                                                 partitionValues : InternalRow,
                                                 requiredFields: Array[StructField],
                                                 partitionSchema : StructType,
                                                 isFullAcidTable: Boolean)
                  extends InputPartition[ColumnarBatch] {
  override def preferredLocations: Array[String] = {
    try split.inputSplit.value.getLocations
    catch {
      case e: Exception =>
        //preferredLocations specifies to return empty array if no preference
        new Array[String] (0)
    }
  }

  override def createPartitionReader: InputPartitionReader[ColumnarBatch] = {
    new HiveAcidInputPartitionReaderV2(split, broadcastedConf, partitionValues,
      requiredFields, partitionSchema, isFullAcidTable)
  }
}
