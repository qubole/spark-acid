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

import com.qubole.spark.datasources.hiveacid.sql.execution.SparkAcidSqlParser
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import com.qubole.spark.hiveacid.datasource.{HiveAcidDataSource, HiveAcidDataSourceV2}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.HiveSerDe


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

  private def convert(relation: HiveTableRelation): LogicalPlan = {
    val options = relation.tableMeta.properties ++
      relation.tableMeta.storage.properties ++ Map("table" -> relation.tableMeta.qualifiedName)
    val newRelation = new HiveAcidDataSource().createRelation(spark.sqlContext, options)
    LogicalRelation(newRelation, isStreaming = false)
  }

  private def convertV2(relation: HiveTableRelation): LogicalPlan = {
    val serde = relation.tableMeta.storage.serde.getOrElse("")
    if (!serde.equalsIgnoreCase(HiveSerDe.sourceToSerDe("orc").get.serde.get)) {
      // Only ORC formatted is supported as of now. If its not ORC, then fallback to
      // datasource V1.
      logInfo("Falling back to datasource v1 as " + serde + " is not supported by v2 reader.")
      return convert(relation)
    }
    val dbName = relation.tableMeta.identifier.database.getOrElse("default")
    val tableName = relation.tableMeta.identifier.table
    val tableOpts = Map("database" -> dbName, "table" -> tableName)
    DataSourceV2Relation.create(new HiveAcidDataSourceV2, tableOpts, None, None)
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
          if (spark.conf.get("spark.hive.acid.datasource.version", "v1").equals("v2")) {
            convertV2(relation)
          } else {
            convert(relation)
          }
    }
  }
}

class HiveAcidAutoConvertExtension extends (SparkSessionExtensions => Unit) {
  def apply(extension: SparkSessionExtensions): Unit = {
    extension.injectResolutionRule(HiveAcidAutoConvert.apply)
    extension.injectParser { (session, parser) =>
      SparkAcidSqlParser(parser)
    }
  }
}
