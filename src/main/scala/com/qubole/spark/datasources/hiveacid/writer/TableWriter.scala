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

package com.qubole.spark.datasources.hiveacid.writer

import com.qubole.shaded.hadoop.hive.conf.HiveConf
import com.qubole.shaded.hadoop.hive.ql.plan.FileSinkDesc
import com.qubole.spark.datasources.hiveacid.util.SerializableConfiguration
import com.qubole.spark.datasources.hiveacid.{HiveAcidOperation, HiveAcidTable, HiveAcidTxnManager}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.implicitConversions

class TableWriter(sparkSession: SparkSession,
                  hiveAcidTable: HiveAcidTable,
                  hiveConf: HiveConf,
                  operationType: HiveAcidOperation.OperationType,
                  df: DataFrame,
                  dfHasRowIds: Boolean
                 ) {

  private val txnManager = new HiveAcidTxnManager(sparkSession, hiveConf, hiveAcidTable,
    operationType)

  def writeToTable(): Unit = {
    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    val tableColumnNames = if (dfHasRowIds) {
      hiveAcidTable.tableSchemaWithRowId.fields.map(_.name)
    } else {
      hiveAcidTable.tableSchema.fields.map(_.name)
    }

    val allColumns = df.queryExecution.logical.output.zip(tableColumnNames).map {
      case (attr, tableColumnName) =>
        attr.withName(tableColumnName)
    }

    val partitionColumns = hiveAcidTable.partitionSchema.fields.map(
      field => UnresolvedAttribute.quoted(field.name))
    val partitionSet = AttributeSet(partitionColumns)
    val dataColumns = allColumns.filterNot(partitionSet.contains)

    try {
      startTransaction()
      val writerOptions = new HiveAcidWriterOptions(
        currentWriteId = getCurrentWriteIdForTable(),
        operationType = operationType,
        fileSinkConf = fileSinkDescriptor,
        serializableHadoopConf = new SerializableConfiguration(hadoopConf),
        dataColumns = dataColumns,
        partitionColumns = partitionColumns,
        allColumns = allColumns,
        rootPath = hiveAcidTable.rootPath.toUri.toString,
        timeZoneId = sparkSession.sessionState.conf.sessionLocalTimeZone
      )

      df.queryExecution.executedPlan.execute().foreachPartition {
        iterator =>
          val writer = new HiveAcidWriter(writerOptions)
          iterator.foreach { row => writer.process(row) }
          writer.close()
      }
      endTransaction()
    } catch {
      case e: Exception =>
        endTransaction(true)
        throw e
    }
  }

  lazy val fileSinkDescriptor: FileSinkDesc = {
    val fileSinkDesc = new FileSinkDesc()
    fileSinkDesc.setDirName(hiveAcidTable.rootPath)
    fileSinkDesc.setTableInfo(hiveAcidTable.tableDesc)
    fileSinkDesc.setTableWriteId(getCurrentWriteIdForTable())
    if (operationType == HiveAcidOperation.INSERT_OVERWRITE) {
      fileSinkDesc.setInsertOverwrite(true)
    }
    fileSinkDesc
  }

  private def startTransaction(): Unit = {
    txnManager.begin(Seq())
  }

  private def getCurrentWriteIdForTable(): Long = {
    txnManager.getCurrentWriteIdForTable()
  }

  private def endTransaction(abort: Boolean = false): Unit = {
    txnManager.end(abort)
  }

}


