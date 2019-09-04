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

package com.qubole.spark.datasources.hiveacid

import java.util.Locale

import com.qubole.spark.datasources.hiveacid.sql.HiveAnalysisException
import com.qubole.spark.datasources.hiveacid.sql.catalyst.plans.logical.{Delete, Update}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, InsertIntoTable, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.{FindDataSourceTable, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf


/**
 * Analyzer rule to convert a transactional HiveRelation
 * into LogicalRelation backed by HiveAcidRelation
 * @param spark - spark session
 */
case class HiveAcidAnalyzer(spark: SparkSession) extends Rule[LogicalPlan] {

  private lazy val analyzer = spark.sessionState.analyzer
  private lazy val resolveRelations = analyzer.ResolveRelations
  private lazy val findDataSourceTable = analyzer.extendedResolutionRules.find(_.isInstanceOf[FindDataSourceTable]).get

  private def isConvertible(relation: HiveTableRelation): Boolean = {
    val serde = relation.tableMeta.storage.serde.getOrElse("").toLowerCase(Locale.ROOT)
    serde.contains("orc") &&
      SQLConf.get.getConfString("spark.sql.hiveAcid.autoConvertHiveAcidTables", "true").toBoolean &&
      relation.tableMeta.properties.getOrElse("transactional", "false").toBoolean
  }

  private def isOrcProperty(key: String) =
    key.startsWith("orc.") || key.contains(".orc.")

  private def convert(relation: HiveTableRelation): LogicalRelation = {
    val options = relation.tableMeta.properties.filterKeys(isOrcProperty) ++
      relation.tableMeta.storage.properties ++ Map("table" -> relation.tableMeta.qualifiedName)

    val newRelation = new HiveAcidDataSource().createRelation(spark.sqlContext, options)
    LogicalRelation(newRelation, false)
  }

  private def convertTable(r1: LogicalPlan): LogicalRelation = {
    val r2 = resolveRelations.resolveRelation(r1)
    val r3 = findDataSourceTable(r2)
    val r4 = r3 match {
      case alias: SubqueryAlias => alias.child.asInstanceOf[HiveTableRelation]
      case _ => r3.asInstanceOf[HiveTableRelation]
    }
    if (!DDLUtils.isHiveTable(r4.tableMeta) || !isConvertible(r4)) {
      throw new HiveAnalysisException(s"Can't convert $r4 to HiveAcidRelation")
    }
    convert(r4)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      // Write path
      case InsertIntoTable(r: HiveTableRelation, partition, query, overwrite, ifPartitionNotExists)
        // Inserting into partitioned table is not supported in Parquet/Orc data source (yet).
        if query.resolved && DDLUtils.isHiveTable(r.tableMeta) && isConvertible(r) =>
        InsertIntoTable(convert(r), partition, query, overwrite, ifPartitionNotExists)

      case Delete(table, filter, false) =>
        val relation = convertTable(table)
        val newFilter = filter.map(_.asInstanceOf[Filter].copy(child = relation))
        // TODO write a DataWritingCommand
        Delete(relation, newFilter, tableConverted = true)

      case Update(table, fieldValues, filter, false) =>
        val relation = convertTable(table)
        val newFilter = filter.map(_.asInstanceOf[Filter].copy(child = relation))
        // TODO write a DataWritingCommand
        Update(relation, fieldValues, newFilter, tableConverted = true)

      // Read path
      case relation: HiveTableRelation
        if DDLUtils.isHiveTable(relation.tableMeta) && isConvertible(relation) =>
        convert(relation)
    }
  }
}

