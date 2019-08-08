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

import com.qubole.shaded.hadoop.hive.ql.plan.FileSinkDesc
import com.qubole.spark.datasources.hiveacid.util.SerializableConfiguration
import com.qubole.spark.datasources.hiveacid.writer.{HiveAcidWriter, HiveAcidWriterOptions}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.sources._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class HiveAcidDataSource
  extends RelationProvider
    with CreatableRelationProvider
    with DataSourceRegister
    with Logging {

  override def createRelation(
   sqlContext: SQLContext,
   parameters: Map[String, String]): BaseRelation = {
    new HiveAcidRelation(
      sqlContext,
      parameters
    )
  }

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    val relation = createRelation(sqlContext, parameters).asInstanceOf[HiveAcidRelation]
    val hiveAcidTable = relation.hiveAcidTable
    val hiveConf = relation.hiveConf
    val tableDesc = relation.hiveAcidTable.tableDesc

    val operationType = if (mode == SaveMode.Overwrite) {
      HiveAcidOperation.INSERT_OVERWRITE
    } else {
      HiveAcidOperation.INSERT_INTO
    }

    val txnManager = new HiveAcidTxnManager(sqlContext.sparkSession, hiveConf, hiveAcidTable,
      operationType)
    txnManager.begin(Seq())
    val currentWriteId = txnManager.allocateTableWriteId()
    val fsc = new FileSinkDesc()
    fsc.setDirName(hiveAcidTable.rootPath)
    fsc.setTableInfo(tableDesc)
    if (mode == SaveMode.Overwrite) {
      fsc.setInsertOverwrite(true)
    }
    fsc.setTableWriteId(currentWriteId)
    val hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()

    val tableColumnNames = hiveAcidTable.schema.fields.map(_.name)
    val allColumns = data.queryExecution.logical.output.zip(tableColumnNames).map {
      case (attr, tableColumnName) =>
        attr.withName(tableColumnName)
    }
    val partitionColumns = hiveAcidTable.partitionSchema.fields.map(
      field => UnresolvedAttribute.quoted(field.name))
    val partitionSet = AttributeSet(partitionColumns)
    val dataColumns = allColumns.filterNot(partitionSet.contains)

    val writerOptions = new HiveAcidWriterOptions(
      currentWriteId = currentWriteId,
      operationType = operationType,
      fileSinkConf = fsc,
      serializableHadoopConf = new SerializableConfiguration(hadoopConf),
      dataColumns = dataColumns,
      partitionColumns = partitionColumns,
      allColumns = allColumns,
      rootPath = hiveAcidTable.rootPath.toUri.toString,
      timeZoneId = sqlContext.sparkSession.sessionState.conf.sessionLocalTimeZone
    )
    data.queryExecution.executedPlan.execute().foreachPartition {
      iterator =>
        val writer = new HiveAcidWriter(writerOptions)
        iterator.foreach { row => writer.write(row) }
        writer.close()
    }
    txnManager.end()
    relation
  }

  override def shortName(): String = {
    HiveAcidUtils.NAME
  }
}
