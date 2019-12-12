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
import org.apache.spark.sql._
import org.apache.spark.sql.sources._

import com.qubole.spark.hiveacid.{HiveAcidTable, HiveAcidErrors}

/**
  * HiveAcid Data source implementation.
  */
class HiveAcidDataSource
  extends RelationProvider          // USING HiveAcid
    with CreatableRelationProvider  // Insert into/overwrite
    with DataSourceRegister         // FORMAT("HiveAcid")
    with Logging {

  // returns relation for passed in table name
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    HiveAcidRelation(sqlContext.sparkSession, getFullyQualifiedTableName(parameters), parameters)
  }

  // returns relation after writing passed in data frame. Table name is part of parameter
  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              df: DataFrame): BaseRelation = {

    val hiveAcidTable: HiveAcidTable = HiveAcidTable.fromSparkSession(
      sqlContext.sparkSession,
      getFullyQualifiedTableName(parameters),
      parameters)

    mode match {
      case SaveMode.Overwrite =>
        hiveAcidTable.insertOverwrite(df)
      case SaveMode.Append =>
        hiveAcidTable.insertInto(df)
      // TODO: Add support for these
      case SaveMode.ErrorIfExists | SaveMode.Ignore =>
        HiveAcidErrors.unsupportedSaveMode(mode)
    }
    createRelation(sqlContext, parameters)
  }

  override def shortName(): String = {
    HiveAcidDataSource.NAME
  }

  private def getFullyQualifiedTableName(parameters: Map[String, String]): String = {
    parameters.getOrElse("table", {
      throw HiveAcidErrors.tableNotSpecifiedException()
    })
  }
}

object HiveAcidDataSource {
  val NAME = "HiveAcid"
}

