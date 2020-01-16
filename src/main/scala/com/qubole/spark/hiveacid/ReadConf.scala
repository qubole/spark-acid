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

package com.qubole.spark.hiveacid

import org.apache.spark.sql.SparkSession

/**
  * Spark specific configuration container to be used by Hive Acid module
  */
case class ReadConf(predicatePushdownEnabled: Boolean = true,
                    metastorePartitionPruningEnabled: Boolean = true,
                    var includeRowIds: Boolean = false)

object ReadConf {

  val PREDICATE_PUSHDOWN_CONF = "spark.sql.hiveAcid.enablePredicatePushdown"

  def build(sparkSession: SparkSession, parameters: Map[String, String]): ReadConf = {
    val isPredicatePushdownEnabled: Boolean = {
      val sqlConf = sparkSession.sessionState.conf
      sqlConf.getConfString(PREDICATE_PUSHDOWN_CONF, "true") == "true"
    }
    new ReadConf(
      isPredicatePushdownEnabled,
      sparkSession.sessionState.conf.metastorePartitionPruning,
      parameters.getOrElse("includeRowIds", "false").toBoolean
    )
  }
}

