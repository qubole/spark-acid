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

package com.qubole.spark.datasources.hiveacid.reader

import scala.reflect.ClassTag

import com.qubole.spark.datasources.hiveacid.HiveAcidOperation
import com.qubole.spark.datasources.hiveacid.transaction.HiveAcidTxn

import org.apache.spark._
import org.apache.spark.rdd.{RDD, UnionRDD}

/**
  * A Hive3RDD is created for each of the hive partition of the table. But at the end the buildScan
  * is supposed to return only 1 RDD for entire table. So we have to create UnionRDD for it.
  *
  * This class extends UnionRDD and makes sure that we acquire read lock once for all the
  * partitions of the table

  * @param sc - sparkContext
  * @param rddSeq - underlying partition RDDs
  * @param partitionList - partitions represented by this Union RDD
  * @param txn - HiveAcidTxn to take read locks
  */
private[reader] class HiveAcidUnionRDD[T: ClassTag](
   sc: SparkContext,
   rddSeq: Seq[RDD[T]],
   partitionList: Seq[String],
   @transient val txn: HiveAcidTxn) extends UnionRDD[T](sc, rddSeq) {

  override def getPartitions: Array[Partition] = {
    // Initialize the ACID state here to get the write-ids to read
    // Start transaction
    txn.begin()
    txn.acquireLocks(HiveAcidOperation.READ, partitionList)
    super.getPartitions
  }
}
