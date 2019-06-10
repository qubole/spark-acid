package com.qubole.spark.rdd

import com.qubole.spark.HiveAcidState
import org.apache.spark._
import org.apache.spark.rdd.{RDD, UnionRDD}

import scala.reflect.ClassTag

class AcidLockUnionRDD[T: ClassTag](
   sc: SparkContext,
   rddSeq: Seq[RDD[T]],
   partitionList: Seq[String],
   @transient val acidState: HiveAcidState) extends UnionRDD[T](sc, rddSeq) {

  override def getPartitions: Array[Partition] = {
    acidState.begin(partitionList)
    super.getPartitions
  }
}
