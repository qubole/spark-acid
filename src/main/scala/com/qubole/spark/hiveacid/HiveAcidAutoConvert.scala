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

import java.util.Locale

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, Filter}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.LogicalRelation

import com.qubole.spark.hiveacid.datasource.HiveAcidDataSource


/**
 * Analyzer rule to convert a transactional HiveRelation
 * into LogicalRelation backed by HiveAcidRelation
 * @param spark - spark session
 */
case class HiveAcidAutoConvert(spark: SparkSession) extends Rule[LogicalPlan] {

  private def isConvertible(relation: HiveTableRelation): Boolean = {
    val serde = relation.tableMeta.storage.serde.getOrElse("").toLowerCase(Locale.ROOT)
    relation.tableMeta.properties.getOrElse("transactional", "false").toBoolean
  }

  private def convert(relation: HiveTableRelation): LogicalRelation = {
    val options = relation.tableMeta.properties ++
      relation.tableMeta.storage.properties ++ Map("table" -> relation.tableMeta.qualifiedName)

    val newRelation = new HiveAcidDataSource().createRelation(spark.sqlContext, options)
    LogicalRelation(newRelation, isStreaming = false)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      // Write path
      case InsertIntoTable(r: HiveTableRelation, partition, query, overwrite, ifPartitionNotExists)
        if query.resolved && DDLUtils.isHiveTable(r.tableMeta) && isConvertible(r) =>
        InsertIntoTable(convert(r), partition, query, overwrite, ifPartitionNotExists)

      // Read path
      case relation: HiveTableRelation
        if DDLUtils.isHiveTable(relation.tableMeta) && isConvertible(relation) =>
        convert(relation)
    }
  }
}

class HiveAcidAutoConvertExtension extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectResolutionRule(HiveAcidAutoConvert.apply)
  }
}
