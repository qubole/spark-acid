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

import scala.language.implicitConversions

import com.qubole.spark.datasources.hiveacid._
import com.qubole.spark.datasources.hiveacid.transaction.{HiveAcidFullTxn, HiveAcidTxnManager}
import com.qubole.spark.datasources.hiveacid.util.SerializableConfiguration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Performs eager write of a dataframe df to a hive acid table based on operationType
 * @param sparkSession  - Spark session
 * @param hiveAcidMetadata - hive acid table where we want to write dataframe
 */
private[hiveacid] class TableWriter(sparkSession: SparkSession,
                                    txnManager: HiveAcidTxnManager,
                                    hiveAcidMetadata: HiveAcidMetadata) extends Logging {

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

    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    val columnNames = if (expectRowIdsInDataFrame) {
      hiveAcidMetadata.tableSchemaWithRowId.fields.map(_.name)
    } else {
      hiveAcidMetadata.tableSchema.fields.map(_.name)
    }

    val allColumns = df.queryExecution.optimizedPlan.output.zip(columnNames).map {
      case (attr, columnName) =>
        attr.withName(columnName)
    }

    val allColumnNameToAttrMap = allColumns.map(attr => attr.name -> attr).toMap

    val partitionColumns = hiveAcidMetadata.partitionSchema.fields.map(
      field => allColumnNameToAttrMap(field.name))

    val dataColumns = allColumns.filterNot(partitionColumns.contains)

    // Start full transaction
    val txn = new HiveAcidFullTxn(hiveAcidMetadata, txnManager)
    try {
      txn.begin()
      txn.acquireLocks(operationType, Seq())

      val writerOptions = new WriterOptions(txn.currentWriteId,
        operationType,
        new SerializableConfiguration(hadoopConf),
        dataColumns,
        partitionColumns,
        allColumns,
        sparkSession.sessionState.conf.sessionLocalTimeZone
      )

      df.queryExecution.executedPlan.execute().foreachPartition {
        iterator =>
          val writer = Writer.getHive3Writer(hiveAcidMetadata, writerOptions)
          iterator.foreach { row => writer.process(row) }
          writer.close()
      }
      txn.end()
    } catch {
      case e: Exception =>
        logError("Exception", e)
        txn.end(true)
        throw e
    }
  }
}


