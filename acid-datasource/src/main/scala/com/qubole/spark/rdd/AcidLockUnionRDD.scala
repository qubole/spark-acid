package com.qubole.spark.rdd

import com.qubole.spark.HiveAcidState
import org.apache.spark._
import org.apache.spark.rdd.{RDD, UnionRDD}

import scala.reflect.ClassTag

class AcidLockUnionRDD[T: ClassTag](
   sc: SparkContext,
   rddSeq: Seq[RDD[T]],
   partitionList: Seq[String],
   @transient acidState: HiveAcidState) extends UnionRDD[T](sc, rddSeq) {

  def getAcidState(): HiveAcidState = {
    acidState
  }

  override def getPartitions: Array[Partition] = {
    acidState.begin(partitionList)
    super.getPartitions
  }
}
