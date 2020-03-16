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

package com.qubole.spark.hiveacid.streaming

import java.util.Locale

import com.qubole.shaded.hadoop.hive.ql.metadata.InvalidTableException
import com.qubole.spark.hiveacid.{AnalysisException, Table}
import com.qubole.spark.hiveacid.datasource.HiveAcidDataSource
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.OutputMode


class HiveAcidSinkSuite extends HiveAcidStreamingFunSuite {

  override protected def afterAll(): Unit = {
    helper.destroy()
  }

  test("table not created") {
    val ds = new HiveAcidDataSource()
    val tableName = "tempTable"
    val options = Map("table" -> s"$tableName")

    val errorMessage = intercept[InvalidTableException] {
      ds.createSink(helper.spark.sqlContext, options, Seq.empty, OutputMode.Append())
    }.getMessage()
    val expectedMsg = s"""table not found $tableName"""
    assert(errorMessage.toLowerCase(Locale.ROOT).contains(expectedMsg.toLowerCase(Locale.ROOT)))

  }

  test("table not acid table") {
    val ds = new HiveAcidDataSource()
    val tableName = s"tempTable"
    val options = Map("table" -> s"$DEFAULT_DBNAME.$tableName")

    val tType = Table.orcTable
    val cols = Map(
      ("value1","int"),
      ("value2", "int")
    )

    val tableHive = new Table(DEFAULT_DBNAME, tableName, cols, tType, false)

    helper.recreate(tableHive, false)

    val errorMessage = intercept[IllegalArgumentException] {
      ds.createSink(helper.spark.sqlContext, options, Seq.empty, OutputMode.Append())
    }.getMessage()
    val expectedMsg = s"""table ${tableHive.hiveTname} is not an acid table"""
    assert(errorMessage.toLowerCase(Locale.ROOT).contains(expectedMsg.toLowerCase(Locale.ROOT)))

  }

  test("table is bucketed") {
    val ds = new HiveAcidDataSource()
    val tableName = s"tempTable"
    val options = Map("table" -> s"$DEFAULT_DBNAME.$tableName")

    val tType = Table.orcBucketedFullACIDTable
    val cols = Map(
      ("value1","int"),
      ("value2", "int")
    )

    val tableHive = new Table(DEFAULT_DBNAME, tableName, cols, tType, false)

    helper.recreate(tableHive, false)

    val errorMessage = intercept[RuntimeException] {
      ds.createSink(helper.spark.sqlContext, options, Seq.empty, OutputMode.Append())
    }.getMessage()
    val expectedMsg = s"""Unsupported operation type - Streaming Write for Bucketed table """
    assert(errorMessage.toLowerCase(Locale.ROOT).contains(expectedMsg.toLowerCase(Locale.ROOT)))

  }

  test("partitionBy is specified with Acid Streaming") {
    val ds = new HiveAcidDataSource()
    val options = Map("table" -> "dummyTable")
    val errorMessage = intercept[UnsupportedOperationException] {
      ds.createSink(helper.spark.sqlContext, options, Seq("col1", "col2"), OutputMode.Append())
    }.getMessage()

    val expectedMsg = "Unsupported Function - partitionBy with HiveAcidSink"
    assert(errorMessage.toLowerCase(Locale.ROOT).contains(expectedMsg.toLowerCase(Locale.ROOT)))

  }

  test("incorrect output mode is used with Acid Streaming") {
    val ds = new HiveAcidDataSource()
    val options = Map("table" -> "dummyTable")
    val errorMessage = intercept[AnalysisException] {
      ds.createSink(helper.spark.sqlContext, options, Seq.empty, OutputMode.Update())
    }.getMessage()
    val expectedMsg = "mode is Update: Hive Acid Sink supports only Append as OutputMode"
    assert(errorMessage.toLowerCase(Locale.ROOT).contains(expectedMsg.toLowerCase(Locale.ROOT)))

  }

  test("table not specified") {
    val ds = new HiveAcidDataSource()
    val options = Map.empty[String, String]
    val errorMessage = intercept[IllegalArgumentException] {
      ds.createSink(helper.spark.sqlContext, options, Seq.empty, OutputMode.Append())
    }.getMessage()
    val expectedMsg = """Table Name is not specified"""
    assert(errorMessage.toLowerCase(Locale.ROOT).contains(expectedMsg.toLowerCase(Locale.ROOT)))

  }

  // Test Run
  streamingTestForAcidTables(Table.allNonBucketedFullAcidTypes())
  streamingTestForAcidTables(Table.allNonBucketedInsertOnlyTypes())

  def streamingTestForAcidTables(tTypes: List[(String,Boolean)]): Unit = {
    tTypes.foreach { case (tType, isPartitioned) =>
      val tableNameHive = "tHive"
      val testName = s"Simple Streaming Query Append for $tableNameHive type $tType"
      test(testName) {
        val cols: Map[String, String] = {
          if(!isPartitioned) {
            Map(
              ("value1","int"),
              ("value2","int")
            )
          } else {
            Map(
              ("value","int")
            )
          }
        }

        val tableHive = new Table(DEFAULT_DBNAME, tableNameHive, cols, tType, isPartitioned)
        def code(): Unit = {
          helper.recreate(tableHive)

          helper.runStreaming(tableHive.hiveTname, OutputMode.Append(), tableHive.getColMap.keys.toSeq, Range(1, 4))
          val resDf = helper.sparkGetDF(tableHive)
          val resultRow = (Row(100, 10, 1) :: Row(200, 20, 2) :: Row(300, 30, 3) :: Nil).toArray
          helper.compareResult(resDf._1.collect(), resultRow)
          helper.compareResult(resDf._2.collect(), resultRow)
          helper.compare(tableHive, "compare via hive")
        }
        helper.myRun(testName, code)
      }
    }
  }

}
