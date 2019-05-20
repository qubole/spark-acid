package com.qubole.spark.rdd

import org.apache.hadoop.mapred.{FileSplit, InputSplit}
import org.apache.spark.{Partition, SerializableWritable}

import scala.collection.immutable.Map

class HivePartition(rddId: Int, override val index: Int, s: Array[Byte])
  extends Partition {

  val inputSplit: Array[Byte] = s

  def getInputSplitBytes(): Array[Byte] = inputSplit

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}

