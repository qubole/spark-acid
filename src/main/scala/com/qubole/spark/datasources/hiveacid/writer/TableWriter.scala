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

import com.qubole.spark.datasources.hiveacid._
import com.qubole.spark.datasources.hiveacid.transaction.{HiveAcidFullTxn, HiveAcidTxnManager}
import com.qubole.spark.datasources.hiveacid.util.SerializableConfiguration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command.AlterTableAddPartitionCommand
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.language.implicitConversions

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
      // TODO: Tighten this check once we add support for insert into / insert overwrite in
      //  insertonly tables.
      throw HiveAcidErrors.unsupportedOperationTypeInsertOnlyTable(operationType.toString)
    }
  }

  private def getColumns(operationType: HiveAcidOperation.OperationType,
            df: DataFrame): (Seq[Attribute], Array[Attribute], Seq[Attribute]) = {

    val expectRowIdsInDataFrame = operationType match {
      case HiveAcidOperation.INSERT_OVERWRITE | HiveAcidOperation.INSERT_INTO => false
      case _ => throw HiveAcidErrors.invalidOperationType(operationType.toString)
    }

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

    (allColumns, partitionColumns, dataColumns)
  }

  /**
    * Common utility function to perform all types of writes into hive acid table
    * @param operationType type of operation.
    * @param df data frame to be written into the table.
    */
  def write(operationType: HiveAcidOperation.OperationType,
            df: DataFrame): Unit = {

    assertOperation(operationType)

    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    val (allColumns, partitionColumns, dataColumns) = getColumns(operationType, df)

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

      val isFullAcidTable = hiveAcidMetadata.isFullAcidTable

      val hive3Options = WriterOptions.getHiveAcidWriterOptions(hiveAcidMetadata, writerOptions)

      // This RDD is serialized and sent for distributed execution.
      // All the access object in this needs to be serializable.
      val processRddPartition = new (Iterator[InternalRow] => Seq[TablePartitionSpec]) with
        Serializable {
        override def apply(iterator: Iterator[InternalRow]): Seq[TablePartitionSpec] = {
          val writer = if (isFullAcidTable) {
            new HiveAcidFullAcidWriter(writerOptions, hive3Options)
          } else {
            new HiveAcidInsertOnlyWriter(writerOptions, hive3Options)
          }
          iterator.foreach { row => writer.process(row) }
          writer.close()
          writer.partitionsTouched()
        }
      }

      val touchedPartitions = sparkSession.sparkContext.runJob(
        df.queryExecution.executedPlan.execute(), processRddPartition
      ).flatten.toSet

      // Add new partition to table metadata under the transaction.
      val existingPartitions = hiveAcidMetadata.getRawPartitions()
        .map(_.getSpec)
        .map(_.asScala.toMap)

      val newPartitions = touchedPartitions -- existingPartitions

      logInfo(s"existing partitions: ${touchedPartitions.size}, " +
        s"partitions touched: ${touchedPartitions.size}, " +
        s"new partitions to add to metastore: ${newPartitions.size}")

      if (newPartitions.nonEmpty) {
        AlterTableAddPartitionCommand(
          new TableIdentifier(hiveAcidMetadata.tableName, Option(hiveAcidMetadata.dbName)),
          newPartitions.toSeq.map(p => (p, None)),
          ifNotExists = true).run(sparkSession)
      }

      logDebug("new partitions added successfully")
      txn.end()

    } catch {
      case e: Exception =>
        logError("Exception", e)
        txn.end(true)
        throw e
    }
  }
}


