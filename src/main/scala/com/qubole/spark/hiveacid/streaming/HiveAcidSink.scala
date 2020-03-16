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


package com.qubole.spark.hiveacid.streaming

import com.qubole.spark.hiveacid.{HiveAcidErrors, HiveAcidTable}
import com.qubole.spark.hiveacid.hive.HiveAcidMetadata
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.streaming.Sink


class HiveAcidSink(sparkSession: SparkSession,
                   parameters: Map[String, String]) extends Sink with Logging {

  import HiveAcidSink._

  private val acidSinkOptions = new HiveAcidSinkOptions(parameters)

  private val fullyQualifiedTableName = acidSinkOptions.tableName

  private val hiveAcidTable: HiveAcidTable = HiveAcidTable.fromSparkSession(
    sparkSession,
    fullyQualifiedTableName,
    parameters)

  assertNonBucketedTable()

  private val logPath = getMetaDataPath()
  private val fileLog = new HiveAcidSinkLog(
    HiveAcidSinkLog.VERSION, sparkSession, logPath.toUri.toString, acidSinkOptions)

  private def assertNonBucketedTable(): Unit = {
    if(hiveAcidTable.isBucketed()) {
      throw HiveAcidErrors.unsupportedOperationTypeBucketedTable("Streaming Write", fullyQualifiedTableName)
    }
  }

  private def getMetaDataPath(): Path = {
    acidSinkOptions.metadataDir match {
      case Some(dir) =>
        new Path(dir)
      case None =>
        logInfo(s"Metadata dir not specified. Using " +
          s"$metadataDirPrefix/_query_default as metadata dir")
        logWarning(s"Please make sure that multiple streaming writes to " +
          s"$fullyQualifiedTableName are not running")
        val tableLocation = HiveAcidMetadata.fromSparkSession(
          sparkSession, fullyQualifiedTableName).rootPath
        new Path(tableLocation, s"$metadataDirPrefix/_query_default")
    }
  }

  /**
    * Adds the batch to the sink. Each batch is transactional in itself
    * @param batchId batch to add
    * @param df dataframe to add as part of batch
    */
  override def addBatch(batchId: Long, df: DataFrame): Unit = {

    if (batchId <= fileLog.getLatest().map(_._1).getOrElse(-1L)) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {

      val commitProtocol = new HiveAcidStreamingCommitProtocol(fileLog)
      val txnId = hiveAcidTable.addBatch(df)
      commitProtocol.commitJob(batchId, txnId)
    }

  }

  override def toString: String = s"HiveAcidSinkV1[$fullyQualifiedTableName]"

}

object HiveAcidSink {

  val metadataDirPrefix = "_acid_streaming"
}
