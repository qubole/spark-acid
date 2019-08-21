/*
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

import com.qubole.spark.datasources.hiveacid.reader.TableReader
import com.qubole.spark.datasources.hiveacid.transaction.HiveAcidTxnManager
import com.qubole.spark.datasources.hiveacid.util.HiveSparkConversionUtil
import com.qubole.spark.datasources.hiveacid.writer.TableWriter
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{DataFrame, _}

import scala.collection.Map

/**
 * Represents a hive acid table and give API to perform operations on top of it
 * @param sparkSession - spark session object
 * @param parameters - additional parameters
 * @param fullyQualifiedTableName - the fully qualified hive acid table name
 */
class HiveAcidTable(sparkSession: SparkSession,
                    parameters: Map[String, String],
                    hiveAcidMetadata: HiveAcidMetadata) extends Logging {

  private val hiveConf = HiveSparkConversionUtil.createHiveConf(sparkSession.sparkContext)
  private val txnManager = new HiveAcidTxnManager(sparkSession, hiveConf)

  def getRdd(requiredColumns: Array[String],
             filters: Array[Filter],
             options: ReadOptions): RDD[Row] = {
    val tableReader = new TableReader(sparkSession, hiveAcidMetadata)
    tableReader.getRdd(requiredColumns, filters, txnManager, options)
  }

  /**
    * Appends a given dataframe df into the hive acid table
    * @param df - dataframe to insert
    */
  def insertInto(df: DataFrame): Unit = {
    val tableWriter = new TableWriter(sparkSession, txnManager, hiveAcidMetadata)
    tableWriter.write(HiveAcidOperation.INSERT_INTO, df)
  }

  /**
    * Overwrites a given dataframe df onto the hive acid table
    * @param df - dataframe to insert
    */
  def insertOverwrite(df: DataFrame): Unit = {
    val tableWriter = new TableWriter(sparkSession, txnManager, hiveAcidMetadata)
    tableWriter.write(HiveAcidOperation.INSERT_OVERWRITE, df)
  }

  /**
    * Delete rows from the hive acid table based on given condition
    * @param condition - condition string to delete rows
    */
  def delete(condition: String): Unit = {

    val df = sparkSession.read.format(HiveAcidDataSource.NAME)
      .options(parameters ++
        Map("includeRowIds" -> "true", "table" -> hiveAcidMetadata.fullyQualifiedName))
      .load()
      .filter(functions.expr(condition))

    val tableWriter = new TableWriter(sparkSession, txnManager, hiveAcidMetadata)
    tableWriter.write(HiveAcidOperation.DELETE, df)
  }

  /**
    * Update rows in the hive acid table based on condition and newValues
    * @param condition - condition string to identify rows which needs to be updated
    * @param newValues - Map of (column, value) to set
    */
  def update(condition: String, newValues: Map[String, String]): Unit = {

    val df = sparkSession.read.format(HiveAcidDataSource.NAME)
      .options(parameters ++
        Map("includeRowIds" -> "true", "table" -> hiveAcidMetadata.fullyQualifiedName))
      .load()
      .filter(functions.expr(condition))

    // FIXME: Handle table.column in newValues
    def toStrColumnMap(map: Map[String, String]): Map[String, Column] = {
      map.toSeq.map { case (k, v) => k -> functions.expr(v) }.toMap
    }
    val strColumnMap = toStrColumnMap(newValues)
    val updateExpressions: Seq[Expression] =
      df.queryExecution.optimizedPlan.output.map {
        case attr =>
          if (strColumnMap.contains(attr.name)) {
            strColumnMap(attr.name).expr
          } else {
            attr
          }
      }
    val newColumns = updateExpressions.zip(df.queryExecution.optimizedPlan.output).map {
      case (newExpr, origAttr) =>
        new Column(Alias(newExpr, origAttr.name)())
    }
    val updateDf = df.select(newColumns: _*)


    val tableWriter = new TableWriter(sparkSession, txnManager, hiveAcidMetadata)
    tableWriter.write(HiveAcidOperation.UPDATE, updateDf)
  }
}

object HiveAcidTable {
  def fromSparkSession(sparkSession: SparkSession,
                       parameters: Map[String, String],
                       fullyQualifiedTableName: String): HiveAcidTable = {

    val hiveAcidMetadata = HiveAcidMetadata.fromSparkSession(sparkSession, fullyQualifiedTableName)
    new HiveAcidTable(sparkSession, parameters, hiveAcidMetadata)
  }
}