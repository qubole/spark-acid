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

import scala.collection.Map
import com.qubole.spark.datasources.hiveacid.reader.TableReader
import com.qubole.spark.datasources.hiveacid.writer.TableWriter
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.Column
import org.apache.spark.sql.SqlUtils
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExtractValue, GetStructField}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.LogicalRelation


/**
 * Represents a hive acid table and exposes API to perform operations on top of it
 * @param sparkSession - spark session object
 * @param hiveAcidMetadata - metadata object
 * @param parameters - additional parameters
 */
class HiveAcidTable(sparkSession: SparkSession,
                    hiveAcidMetadata: HiveAcidMetadata,
                    parameters: Map[String, String]
                   ) extends Logging {


  /**
    * Return an RDD on top of Hive ACID table
    * @param requiredColumns - columns needed
    * @param filters - filters that can be pushed down to file format
    * @param readConf - read conf
    * @return
    */
  def getRdd(requiredColumns: Array[String],
             filters: Array[Filter],
             readConf: ReadConf): RDD[Row] = {
    val tableReader = new TableReader(sparkSession, hiveAcidMetadata)
    tableReader.getRdd(requiredColumns, filters, readConf)
  }

  /**
    * Appends a given dataframe df into the hive acid table
    * @param df - dataframe to insert
    */
  def insertInto(df: DataFrame): Unit = {
    val tableWriter = new TableWriter(sparkSession, hiveAcidMetadata)
    tableWriter.process(HiveAcidOperation.INSERT_INTO, df)
  }

  /**
    * Overwrites a given dataframe df onto the hive acid table
    * @param df - dataframe to insert
    */
  def insertOverwrite(df: DataFrame): Unit = {
    val tableWriter = new TableWriter(sparkSession, hiveAcidMetadata)
    tableWriter.process(HiveAcidOperation.INSERT_OVERWRITE, df)
  }

  def delete(condition: String): Unit = {

    // Fetch row with rowID in it
    val df = sparkSession.read.format(HiveAcidDataSource.NAME)
      .options(parameters ++
        Map("includeRowIds" -> "true", "table" -> hiveAcidMetadata.fullyQualifiedName))
      .load()

    val resolvedExpr = SqlUtils.resolveReferences(sparkSession,
      functions.expr(condition).expr,
      df.queryExecution.analyzed)

    df.filter(resolvedExpr.sql)

    val tableWriter = new TableWriter(sparkSession, hiveAcidMetadata)
    tableWriter.process(HiveAcidOperation.DELETE, df)
  }

  /**
    * Update rows in the hive acid table based on condition and newValues
    * @param condition - condition string to identify rows which needs to be updated
    * @param newValues - Map of (column, value) to set
    */
  def updateDF(condition: String, newValues: Map[String, String]): DataFrame = {
    // Fetch row with rowID in it
    val df: DataFrame = sparkSession.read.format(HiveAcidDataSource.NAME)
      .options(parameters ++
        Map("includeRowIds" -> "true", "table" -> hiveAcidMetadata.fullyQualifiedName))
      .load()

    val plan = df.queryExecution.analyzed
    val qualifiedPlan = plan match {
      case p : LogicalRelation =>
        p.copy(output = p.output
          .map((x: AttributeReference) =>
            x.withQualifier(hiveAcidMetadata.fullyQualifiedName.split('.').toSeq))
        )
      case _ => plan
    }
    val resolvedExpr = SqlUtils.resolveReferences(sparkSession,
      functions.expr(condition).expr,
      qualifiedPlan)

    val newDf = SqlUtils.convertToDF(sparkSession, qualifiedPlan)

    def toStrColumnMap(map: Map[String, String]): Map[String, Column] = {
      map.toSeq.map { case (k, v) =>
        k -> functions.expr(SqlUtils.resolveReferences(sparkSession, functions.expr(v).expr,
        qualifiedPlan).sql)}.toMap
    }

    val strColumnMap = toStrColumnMap(newValues)
    val updateExpressions: Seq[Expression] =
      newDf.queryExecution.optimizedPlan.output.map {
        attr =>
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

    newDf.filter(resolvedExpr.sql).select(newColumns: _*)
  }

  /**
    * Update rows in the hive acid table based on condition and newValues
    * @param condition - condition string to identify rows which needs to be updated
    * @param newValues - Map of (column, value) to set
    */
  def update(condition: String, newValues: Map[String, String]): Unit = {
    val updateDf = updateDF(condition, newValues)
    val tableWriter = new TableWriter(sparkSession, hiveAcidMetadata)
    tableWriter.process(HiveAcidOperation.UPDATE, updateDf)
  }
}

object HiveAcidTable {
  def fromSparkSession(sparkSession: SparkSession,
                       fullyQualifiedTableName: String,
                       parameters: Map[String, String] = Map()
                      ): HiveAcidTable = {

    val hiveAcidMetadata: HiveAcidMetadata =
      HiveAcidMetadata.fromSparkSession(sparkSession, fullyQualifiedTableName)
    new HiveAcidTable(sparkSession, hiveAcidMetadata, parameters)
  }
}
