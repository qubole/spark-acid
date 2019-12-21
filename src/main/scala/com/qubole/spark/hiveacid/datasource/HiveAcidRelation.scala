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

package com.qubole.spark.hiveacid.datasource

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types._

import com.qubole.spark.hiveacid.{HiveAcidTable, ReadConf}
import com.qubole.spark.hiveacid.hive.HiveAcidMetadata

/**
  * Container for all metadata, configuration and schema to perform operations on
  * Hive ACID datasource. This provides for plumbing most of the heavy lifting is
  * performed inside HiveAcidtTable.
  *
  * @param sparkSession Spark Session object
  * @param fullyQualifiedTableName Table name for the data source.
  * @param parameters user provided parameters required for reading and writing,
  *        including configuration
  */
case class HiveAcidRelation(sparkSession: SparkSession,
                            fullyQualifiedTableName: String,
                            parameters: Map[String, String])
    extends BaseRelation
    with InsertableRelation
    with PrunedFilteredScan
    with Logging {

  private val hiveAcidMetadata: HiveAcidMetadata = HiveAcidMetadata.fromSparkSession(
    sparkSession,
    fullyQualifiedTableName
  )
  private val hiveAcidTable: HiveAcidTable = new HiveAcidTable(sparkSession,
    hiveAcidMetadata, parameters)

  private val readOptions = ReadConf.build(sparkSession, parameters)

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override val schema: StructType = if (readOptions.includeRowIds) {
    hiveAcidMetadata.tableSchemaWithRowId
  } else {
    hiveAcidMetadata.tableSchema
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
   // sql insert into and overwrite
    if (overwrite) {
      hiveAcidTable.insertOverwrite(data)
    } else {
      hiveAcidTable.insertInto(data)
    }
  }

  override def sizeInBytes: Long = {
    val compressionFactor = sparkSession.sessionState.conf.fileCompressionFactor
    (sparkSession.sessionState.conf.defaultSizeInBytes * compressionFactor).toLong
  }

  // FIXME: should it be true / false. Recommendation seems to
  //  be to leave it as true
  override val needConversion: Boolean = false

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val readOptions = ReadConf.build(sparkSession, parameters)
    // sql "select *"
    hiveAcidTable.getRdd(requiredColumns, filters, readOptions)
  }
}
