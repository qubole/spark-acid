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

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class HiveAcidRelation(val sqlContext: SQLContext,
                       fullyQualifiedTableName: String,
                       parameters: Map[String, String])
    extends BaseRelation
    with PrunedFilteredScan
    with Logging {

  val acidTableMetadata: HiveAcidMetadata = HiveAcidMetadata.fromSparkSession(
    sqlContext.sparkSession,
    fullyQualifiedTableName
  )
  val hiveAcidTable: HiveAcidTable = new HiveAcidTable(sqlContext.sparkSession,
    parameters, acidTableMetadata)

  private val readOptions = ReadOptions.build(sqlContext.sparkSession, parameters)

  override val schema: StructType = if (readOptions.includeRowIds) {
    acidTableMetadata.tableSchemaWithRowId
  } else {
    acidTableMetadata.tableSchema
  }

  override def sizeInBytes: Long = {
    val compressionFactor = sqlContext.sparkSession.sessionState.conf.fileCompressionFactor
    (sqlContext.sparkSession.sessionState.conf.defaultSizeInBytes * compressionFactor).toLong
  }

  override val needConversion: Boolean = false

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val readOptions = ReadOptions.build(sqlContext.sparkSession, parameters)
    hiveAcidTable.getRdd(requiredColumns, filters, readOptions)
  }
}