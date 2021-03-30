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

package com.qubole.spark.hiveacid.reader.hive

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import org.apache.hadoop.hive.common.{ValidReadTxnList, ValidTxnList}
import com.qubole.spark.hiveacid.rdd.{HiveAcidPartition, HiveAcidRDD, HiveSplitInfo}
import com.qubole.spark.hiveacid.reader.hive.HiveAcidPartitionComputer.{addToPartitionCache, getInputFormat}
import com.qubole.spark.hiveacid.util.Util
import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, InvalidInputException, JobConf}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

private[hiveacid] case class HiveAcidPartitionComputer(ignoreEmptySplits: Boolean,
                                                  ignoreMissingFiles: Boolean) extends Logging {
  def getPartitions[K, V](id: Int, jobConf: JobConf,
                    inputFormat: InputFormat[K, V],
                    minPartitions: Int): Array[HiveAcidPartition] = {
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    try {
      val allInputSplits = inputFormat.getSplits(jobConf, minPartitions)
      val inputSplits = if (ignoreEmptySplits) {
        allInputSplits.filter(_.getLength > 0)
      } else {
        allInputSplits
      }
      val array = new Array[HiveAcidPartition](inputSplits.length)
      for (i <- inputSplits.indices) {
        array(i) = new HiveAcidPartition(id, i, inputSplits(i))
      }
      array
    } catch {
      case e: InvalidInputException if ignoreMissingFiles =>
        val inputDir = jobConf.get(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR)
        logWarning(s"$inputDir doesn't exist and no" +
          s" partitions returned from this path.", e)
        Array.empty[HiveAcidPartition]
    }
  }

  // needs to be invoked just once as its an expensive operation.
  def computeHiveSplitsAndCache(splitRDD: RDD[HiveSplitInfo]): Unit = {
    val start = System.nanoTime()
    logInfo("Spawning job to compute partitions for ACID table RDD")
    val splits = splitRDD.map {
      case HiveSplitInfo(id, broadcastedConf,
      validTxnList, minPartitions, ifcName, isFullAcidTable, shouldCloneJobConf, initLocalJobConfFuncOpt) =>
        val jobConf = HiveAcidRDD.setInputPathToJobConf(
          Some(HiveAcidRDD.getJobConf(broadcastedConf, shouldCloneJobConf, initLocalJobConfFuncOpt)),
          isFullAcidTable,
          new ValidReadTxnList(validTxnList),
          broadcastedConf,
          shouldCloneJobConf,
          initLocalJobConfFuncOpt)
        val partitions = this.getPartitions[Writable, Writable](id, jobConf, getInputFormat(jobConf, ifcName), minPartitions)
        (partitions, FileInputFormat.getInputPaths(jobConf), validTxnList)
    }.collect()

    splits.foreach {
      case (partitions: Array[HiveAcidPartition],
      paths: Array[Path], validWriteIdList: String) =>
        addToPartitionCache(paths, validWriteIdList, partitions)
    }
    logInfo(s"Job to compute partitions took: " +
      s"${TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start)} seconds")
  }
}

private[hiveacid] object HiveAcidPartitionComputer extends Logging {
  object Cache {
    val partitionCache = new ConcurrentHashMap[SplitCacheKey, Array[HiveAcidPartition]]()
    case class SplitCacheKey(paths: Set[Path], validWriteIdList: String)
  }

  def getFromSplitsCache(paths: Array[Path], validTxnList: ValidTxnList): Option[Array[HiveAcidPartition]] = {
    Option(Cache.partitionCache.get(Cache.SplitCacheKey(paths.toSet, validTxnList.writeToString())))
  }

  def removeFromSplitsCache(paths: Array[Path], validTxnList: ValidTxnList): Unit = {
    Cache.partitionCache.remove(Cache.SplitCacheKey(paths.toSet, validTxnList.writeToString()))
  }

  def addToPartitionCache(paths: Array[Path], validWriteIdList: String, inputSplits: Array[HiveAcidPartition]): Unit = {
    Cache.partitionCache.put(Cache.SplitCacheKey(paths.toSet, validWriteIdList), inputSplits)
  }

  private def getInputFormat(conf: JobConf, inputFormatClassName: String): InputFormat[Writable, Writable] = {
    val inputFormatClass = Util.classForName(inputFormatClassName, loadShaded = true)
      .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
    val newInputFormat = ReflectionUtils.newInstance(inputFormatClass.asInstanceOf[Class[_]], conf)
      .asInstanceOf[InputFormat[Writable, Writable]]
    newInputFormat match {
      case c: Configurable => c.setConf(conf)
      case _ =>
    }
    newInputFormat
  }

}
