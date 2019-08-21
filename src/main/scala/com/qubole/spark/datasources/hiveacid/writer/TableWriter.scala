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

import com.qubole.shaded.hadoop.hive.ql.plan.FileSinkDesc
import com.qubole.spark.datasources.hiveacid._
import com.qubole.spark.datasources.hiveacid.util.{HiveSparkConversionUtil, SerializableConfiguration}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.implicitConversions

/**
 * Performs eager write of a dataframe df to a hive acid table based on operationType
 * @param sparkSession  - Spark session
 * @param hiveAcidMetadata - hive acid table where we want to write dataframe
 */
class TableWriter(sparkSession: SparkSession,
                  hiveAcidMetadata: HiveAcidMetadata) {

  private val hiveConf = HiveSparkConversionUtil.createHiveConf(sparkSession.sparkContext)

  private def assertOperation(operationType: HiveAcidOperation.OperationType): Unit = {
    if (hiveAcidMetadata.isInsertOnlyTable) {
      operationType match {
        case HiveAcidOperation.INSERT_OVERWRITE | HiveAcidOperation.INSERT_INTO =>
        case _ =>
          throw HiveAcidErrors.unsupportedOperationTypeInsertOnlyTable(operationType.toString)
      }
    }
  }

  def write(operationType: HiveAcidOperation.OperationType,
            df: DataFrame): Unit = {
    assertOperation(operationType)
    val expectRowIdsInDataFrame = operationType match {
      case HiveAcidOperation.INSERT_OVERWRITE | HiveAcidOperation.INSERT_INTO => false
      case HiveAcidOperation.UPDATE | HiveAcidOperation.DELETE => true
      case _ => throw HiveAcidErrors.invalidOperationType(operationType.toString)
    }
    val txnManager = new HiveAcidTxnManager(sparkSession, hiveConf, hiveAcidMetadata,
      operationType)
    def startTransaction(): Unit = txnManager.begin(Seq())
    def getCurrentWriteIdForTable: Long = txnManager.getCurrentWriteIdForTable()
    def endTransaction(abort: Boolean = false): Unit = txnManager.end(abort)

    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    val tableColumnNames = if (expectRowIdsInDataFrame) {
      hiveAcidMetadata.tableSchemaWithRowId.fields.map(_.name)
    } else {
      hiveAcidMetadata.tableSchema.fields.map(_.name)
    }

    val allColumns = df.queryExecution.logical.output.zip(tableColumnNames).map {
      case (attr, tableColumnName) =>
        attr.withName(tableColumnName)
    }

    val partitionColumns = hiveAcidMetadata.partitionSchema.fields.map(
      field => UnresolvedAttribute.quoted(field.name))
    val partitionSet = AttributeSet(partitionColumns)
    val dataColumns = allColumns.filterNot(partitionSet.contains)
    lazy val fileSinkDescriptor: FileSinkDesc = {
      val fileSinkDesc = new FileSinkDesc()
      fileSinkDesc.setDirName(hiveAcidMetadata.rootPath)
      fileSinkDesc.setTableInfo(hiveAcidMetadata.tableDesc)
      fileSinkDesc.setTableWriteId(getCurrentWriteIdForTable)
      if (operationType == HiveAcidOperation.INSERT_OVERWRITE) {
        fileSinkDesc.setInsertOverwrite(true)
      }
      fileSinkDesc
    }
    val isFullAcidTable = hiveAcidMetadata.isFullAcidTable

    try {
      startTransaction()
      val writerOptions = new RowWriterOptions(
        currentWriteId = getCurrentWriteIdForTable,
        operationType = operationType,
        fileSinkConf = fileSinkDescriptor,
        serializableHadoopConf = new SerializableConfiguration(hadoopConf),
        dataColumns = dataColumns,
        partitionColumns = partitionColumns,
        allColumns = allColumns,
        rootPath = hiveAcidMetadata.rootPath.toUri.toString,
        timeZoneId = sparkSession.sessionState.conf.sessionLocalTimeZone
      )

      df.queryExecution.executedPlan.execute().foreachPartition {
        iterator =>
          val writer = RowWriter.getRowWriter(writerOptions, isFullAcidTable)
          iterator.foreach { row => writer.process(row) }
          writer.close()
      }
      endTransaction()
    } catch {
      case e: Exception =>
        println(e)
        endTransaction(true)
        throw e
    }
  }
}


