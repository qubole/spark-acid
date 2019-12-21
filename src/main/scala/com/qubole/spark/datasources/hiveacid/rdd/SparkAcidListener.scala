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

package com.qubole.spark.datasources.hiveacid.rdd

import com.qubole.spark.datasources.hiveacid.HiveAcidState
import org.apache.spark.scheduler._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution._
import org.apache.spark.sql.util.QueryExecutionListener

class SparkAcidQueryListener(acidState: HiveAcidState) extends QueryExecutionListener with Logging {
  private val acidStateLocal: HiveAcidState = acidState

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    acidStateLocal.end()
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    acidStateLocal.end()
  }
}

class SparkAcidListener(acidState: HiveAcidState) extends SparkListener {
  private val acidStateLocal: HiveAcidState = acidState

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    super.onApplicationEnd(applicationEnd)
    acidStateLocal.end()
  }
}


