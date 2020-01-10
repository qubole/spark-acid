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

package com.qubole.spark.hiveacid.writer

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import com.qubole.spark.hiveacid._
import com.qubole.spark.hiveacid.hive.HiveAcidMetadata
import com.qubole.spark.hiveacid.writer.hive.{HiveAcidFullAcidWriter, HiveAcidInsertOnlyWriter, HiveAcidWriterOptions}
import com.qubole.spark.hiveacid.transaction._
import com.qubole.spark.hiveacid.util.SerializableConfiguration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command.AlterTableAddPartitionCommand
import org.apache.spark.sql.types.StructType

/**
 * Performs eager write of a dataframe df to a hive acid table based on operationType
 * @param sparkSession  - Spark session
 * @param curTxn - Transaction object to acquire locks.
 * @param hiveAcidMetadata - Hive acid table where we want to write dataframe
 */
private[hiveacid] class TableWriter(sparkSession: SparkSession,
                                    curTxn: HiveAcidTxn,
                                    hiveAcidMetadata: HiveAcidMetadata) extends Logging {

  private def getSchema(operationType: HiveAcidOperation.OperationType): StructType = {
    val expectRowIdsInDataFrame = operationType match {
      case HiveAcidOperation.INSERT_OVERWRITE | HiveAcidOperation.INSERT_INTO => false
      case HiveAcidOperation.DELETE | HiveAcidOperation.UPDATE => true
      case _ => throw HiveAcidErrors.invalidOperationType(operationType.toString)
    }

    if (expectRowIdsInDataFrame) {
      hiveAcidMetadata.tableSchemaWithRowId
    } else {
      hiveAcidMetadata.tableSchema
    }
  }

  private def getColumns(operationType: HiveAcidOperation.OperationType,
            df: DataFrame): (Seq[Attribute], Array[Attribute], Seq[Attribute]) = {

    val columnNames = getSchema(operationType).fields.map(_.name)

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
    * Common utility function to process all types of operations insert/update/delete
    * for the hive acid table
    * @param operationType type of operation.
    * @param df data frame to be written into the table.
    */
  def process(operationType: HiveAcidOperation.OperationType,
              df: DataFrame): Unit = {

    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    val (allColumns, partitionColumns, dataColumns) = getColumns(operationType, df)

    try {

      // FIXME: IF we knew the partition then we should
      //   only lock that partition.
      curTxn.acquireLocks(hiveAcidMetadata, operationType, Seq())

      // Create Snapshot !!!
      val curSnapshot = HiveAcidTxn.createSnapshot(curTxn, hiveAcidMetadata)

      val writerOptions = new WriterOptions(curSnapshot.currentWriteId,
        operationType,
        new SerializableConfiguration(hadoopConf),
        getSchema(operationType),
        dataColumns,
        partitionColumns,
        allColumns,
        sparkSession.sessionState.conf.sessionLocalTimeZone
      )

      val isFullAcidTable = hiveAcidMetadata.isFullAcidTable

      val hiveAcidWriterOptions = HiveAcidWriterOptions.get(hiveAcidMetadata, writerOptions)

      // This RDD is serialized and sent for distributed execution.
      // All the access object in this needs to be serializable.
      val processRddPartition = new (Iterator[InternalRow] => Seq[TablePartitionSpec]) with
        Serializable {
        override def apply(iterator: Iterator[InternalRow]): Seq[TablePartitionSpec] = {
          val writer = if (isFullAcidTable) {
            new HiveAcidFullAcidWriter(writerOptions, hiveAcidWriterOptions)
          } else {
            new HiveAcidInsertOnlyWriter(writerOptions, hiveAcidWriterOptions)
          }
          iterator.foreach { row => writer.process(row) }
          writer.close()
          writer.partitionsTouched()
        }
      }

      val resultRDD =
        operationType match {
          // Deleted rowId needs to be in the same bucketed file name
          // as original row. To achieve that repartition it based on
          // the rowId.bucketId. After this shuffle partitionId maps
          // 1-to-1 with bucketId.
          case HiveAcidOperation.DELETE | HiveAcidOperation.UPDATE =>
            df.sort("rowId.bucketId")
              .toDF.queryExecution.executedPlan.execute()
          case HiveAcidOperation.INSERT_OVERWRITE | HiveAcidOperation.INSERT_INTO =>
            df.queryExecution.executedPlan.execute()
          case unknownOperation =>
            throw HiveAcidErrors.invalidOperationType(unknownOperation.toString)
        }

      val touchedPartitions = sparkSession.sparkContext.runJob(
        resultRDD, processRddPartition
      ).flatten.toSet

      // Add new partition to table metadata under the transaction.
      val existingPartitions = hiveAcidMetadata.getRawPartitions()
        .map(_.getSpec)
        .map(_.asScala.toMap)

      val newPartitions = touchedPartitions -- existingPartitions

      logDebug(s"existing partitions: ${touchedPartitions.size}, " +
        s"partitions touched: ${touchedPartitions.size}, " +
        s"new partitions to add to metastore: ${newPartitions.size}")

      if (newPartitions.nonEmpty) {
        AlterTableAddPartitionCommand(
          new TableIdentifier(hiveAcidMetadata.tableName, Option(hiveAcidMetadata.dbName)),
          newPartitions.toSeq.map(p => (p, None)),
          ifNotExists = true).run(sparkSession)
      }

      // FIXME: Add the notification events for replication et al.
      //

      logDebug("new partitions added successfully")

    } catch {
      case e: Exception =>
        logError("Exception", e)
        throw e
    }
  }
}


