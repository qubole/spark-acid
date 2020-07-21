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

import com.qubole.spark.hiveacid.SparkAcidConf
import com.qubole.spark.hiveacid.reader.hive.HiveAcidPartitionComputer

import scala.reflect.ClassTag
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
  * @param hiveSplitInfo - It is sequence of HiveSplitInfo.
  *                      It would be derived from the list of HiveAcidRDD passed here.
  *                      check HiveAcidRDD.getHiveSplitsInfo
  */
private[hiveacid] class HiveAcidUnionRDD[T: ClassTag](
   sc: SparkContext,
   rddSeq: Seq[RDD[T]],
   //TODO: We should clean so that HiveSplitInfo need not have to be passed separately.
   hiveSplitInfo: Seq[HiveSplitInfo]) extends UnionRDD[T](sc, rddSeq) {

  private val ignoreMissingFiles =
    super.sparkContext.getConf.getBoolean("spark.files.ignoreMissingFiles", defaultValue = false)

  private val ignoreEmptySplits =
    super.sparkContext.getConf.getBoolean("spark.hadoopRDD.ignoreEmptySplits", defaultValue = false)

  private val parallelPartitionThreshold =
    super.sparkContext.getConf.getInt(SparkAcidConf.PARALLEL_PARTITION_THRESHOLD.configName, 10)

  override def getPartitions: Array[Partition] = {
    if (hiveSplitInfo.length > parallelPartitionThreshold) {
      val partitions = hiveSplitInfo.length/parallelPartitionThreshold
      val hiveSplitRDD = super.sparkContext.parallelize(hiveSplitInfo, partitions)
      val hiveAcidPartitionComputer = new HiveAcidPartitionComputer(ignoreEmptySplits, ignoreMissingFiles)
      // It spawns a spark job to compute Partitions for every RDD and stores it in cache.
      hiveAcidPartitionComputer.computeHiveSplitsAndCache(hiveSplitRDD)
    }
    super.getPartitions
  }
}
