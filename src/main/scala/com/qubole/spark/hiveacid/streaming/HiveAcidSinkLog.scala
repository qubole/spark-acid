
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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.CompactibleFileStreamLog

case class HiveAcidSinkStatus(txnId: Long, action: String)

class HiveAcidSinkLog(version: Int,
                      sparkSession: SparkSession,
                      path: String,
                      options: HiveAcidSinkOptions)
  extends CompactibleFileStreamLog[HiveAcidSinkStatus](version, sparkSession, path) {

  protected override val fileCleanupDelayMs = options.fileCleanupDelayMs

  protected override val isDeletingExpiredLog = options.isDeletingExpiredLog

  protected override val defaultCompactInterval = options.compactInterval

  protected override val minBatchesToRetain = options.minBatchesToRetain

  override def compactLogs(logs: Seq[HiveAcidSinkStatus]): Seq[HiveAcidSinkStatus] = {
    val deletedFiles = logs.filter(_.action == HiveAcidSinkLog.DELETE_ACTION).map(_.txnId).toSet
    if (deletedFiles.isEmpty) {
      logs
    } else {
      logs.filter(f => !deletedFiles.contains(f.txnId))
    }
  }

}

object HiveAcidSinkLog {

  val VERSION = 1
  val DELETE_ACTION = "delete"
  val ADD_ACTION = "add"

}