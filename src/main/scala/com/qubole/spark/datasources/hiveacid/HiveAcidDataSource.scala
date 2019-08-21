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
import org.apache.spark.sql._
import org.apache.spark.sql.sources._

class HiveAcidDataSource
  extends RelationProvider
    with CreatableRelationProvider
    with DataSourceRegister
    with Logging {

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {

    val fullyQualifiedTableName = parameters.getOrElse("table", {
      throw HiveAcidErrors.tableNotSpecifiedException
    })

    new HiveAcidRelation(sqlContext, fullyQualifiedTableName, parameters)
  }

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              df: DataFrame): BaseRelation = {

    val hiveAcidTable: HiveAcidTable = HiveAcidTable.fromSparkSession(
      sqlContext.sparkSession,
      parameters,
      getFullyQualifiedTableName(parameters)
    )
    if (mode == SaveMode.Overwrite) {
      hiveAcidTable.insertOverwrite(df)
    } else {
      hiveAcidTable.insertInto(df)
    }

    createRelation(sqlContext, parameters)
  }

  /**
    * Delete data from the underlying table based on given condition
    * @param sqlContext - Spark sql context
    * @param parameters - map containing different configs and parameters including tableName etc
    * @param condition - condition on which data will be deleted. ex - "col1 = 2 and col2 > 3"
    * @return a relation representing the data after deletion
    */
  def deleteRelation(sqlContext: SQLContext,
                     parameters: Map[String, String],
                     condition: String): BaseRelation = {

    val hiveAcidTable: HiveAcidTable = HiveAcidTable.fromSparkSession(
      sqlContext.sparkSession,
      parameters,
      getFullyQualifiedTableName(parameters)
    )
    hiveAcidTable.delete(condition)

    createRelation(sqlContext, parameters)
  }

  override def shortName(): String = {
    HiveAcidDataSource.NAME
  }

  private def getFullyQualifiedTableName(parameters: Map[String, String]): String = {
    parameters.getOrElse("table", {
      throw HiveAcidErrors.tableNotSpecifiedException
    })
  }

//  def updateRelation(sqlContext: SQLContext,
//                     parameters: Map[String, String],
//                     condition: String, set: Map[String, String]): BaseRelation = {
//
//    def toStrColumnMap(map: Map[String, String]): Map[String, Column] = {
//      map.toSeq.map { case (k, v) => k -> functions.expr(v) }.toMap
//    }
//
//    def buildUpdatedColumns(condition: Expression, updateExpressions): Seq[Column] = {
//      updateExpressions.zip(target.output).map { case (update, original) =>
//        val updated = If(condition, update, original)
//        new Column(Alias(updated, original.name)())
//      }
//    }
//
//    val updateValueMap = toStrColumnMap(set)
//    val deleteCondition = functions.expr(condition)
//    val relation = createRelation(sqlContext, parameters ++
//      Map("includeRowIds" -> "true")).asInstanceOf[HiveAcidRelation]
//    val spark = sqlContext.sparkSession
//    val df = spark.read.format("HiveAcid")
//      .options(parameters ++ Map("includeRowIds" -> "true")).load().filter(deleteCondition)
//
//    val hiveAcidTable = relation.hiveAcidTable
//    val hiveConf = relation.hiveConf
//
//    val operationType = HiveAcidOperation.DELETE
//
//    val tableWriter = new TableWriter(sqlContext.sparkSession, hiveAcidTable,
//      hiveConf, operationType, df, true
//    )
//    tableWriter.writeToTable()
//    relation
//  }

}

object HiveAcidDataSource {
  val NAME = "HiveAcid"
}

