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

package com.qubole.spark.hiveacid


import org.apache.log4j.{Level, LogManager, Logger}
import org.scalatest._

import scala.util.control.NonFatal

class UpdateDeleteSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  val log: Logger = LogManager.getLogger(this.getClass)
  log.setLevel(Level.INFO)

  var helper: TestHelper = _
  val isDebug = true

  val DEFAULT_DBNAME =  "HiveTestUpdateDeleteDB"
  val cols: Map[String, String] = Map(
    ("intCol","int"),
    ("doubleCol","double"),
    ("floatCol","float"),
    ("booleanCol","boolean")
  )

  override def beforeAll() {
    try {
      helper = new TestHelper
      if (isDebug) {
        log.setLevel(Level.DEBUG)
      }
      helper.init(isDebug)

      // DB
      helper.hiveExecute("DROP DATABASE IF EXISTS "+ DEFAULT_DBNAME +" CASCADE")
      helper.hiveExecute("CREATE DATABASE "+ DEFAULT_DBNAME)
    } catch {
      case NonFatal(e) => log.info("failed " + e)
    }
  }

  override protected def afterAll(): Unit = {
    helper.destroy()
  }

  val testTables = List(
    // Positive Test
    (Table.orcFullACIDTable, false, true),
    (Table.orcPartitionedFullACIDTable, true, true),
    // Negative Test
    (Table.orcTable, false, false),
    (Table.orcPartitionedTable, true, false),
    (Table.orcBucketedTable, false, false), (Table.orcBucketedPartitionedTable, true, false))
  // Test Run
  updateTestForFullAcidTables(testTables)
  deleteTestForFullAcidTables(testTables)

  // Update test for full acid tables
  def updateTestForFullAcidTables(tTypes: List[(String, Boolean, Boolean)]): Unit = {
    tTypes.foreach { case (tType, isPartitioned, positiveTest) =>
      val tableNameSpark = "tSparkUpdate"
      val testName = s"Update Test for $tableNameSpark type $tType"
      test(testName) {
        val tableSpark = new Table(DEFAULT_DBNAME, tableNameSpark, cols, tType, isPartitioned)
        def code(): Unit = {

          if (positiveTest) {
            helper.recreate(tableSpark)
            helper.hiveExecute(tableSpark.insertIntoHiveTableKeyRange(11, 20))
            val expectedRows = 10
            helper.compareResult(expectedRows.toString, helper.sparkCollect(tableSpark.count))
            val expectedUpdateValue = helper.sparkCollect(tableSpark.selectExpectedUpdateCol(11))
            helper.sparkSQL(tableSpark.updateInHiveTableKey(11))
            val updatedVal = helper.sparkCollect(tableSpark.selectUpdateCol(11))
            helper.compareResult(expectedUpdateValue, updatedVal)
          } else {
            intercept[RuntimeException] {
              helper.recreate(tableSpark)
              helper.sparkSQL(tableSpark.updateInHiveTableKey(11))
            }
          }
        }
        helper.myRun(testName, code)
      }
    }
  }

  // Delete test for full acid tables
  def deleteTestForFullAcidTables(tTypes: List[(String, Boolean, Boolean)]): Unit = {
    tTypes.foreach { case (tType, isPartitioned, positiveTest) =>
      val tableNameSpark = "tSparkDelete"
      val testName = s"Delete Test for $tableNameSpark type $tType"
      test(testName) {
        val tableSpark = new Table(DEFAULT_DBNAME, tableNameSpark, cols, tType, isPartitioned)
        def code(): Unit = {
          if (positiveTest) {
            helper.recreate(tableSpark)
            helper.hiveExecute(tableSpark.insertIntoHiveTableKeyRange(11, 20))
            var expectedRows = 10
            helper.compareResult(expectedRows.toString, helper.sparkCollect(tableSpark.count))

            // delete 1 row
            helper.sparkSQL(tableSpark.deleteFromHiveTableKey(11))
            expectedRows = 9
            helper.compareResult(expectedRows.toString, helper.sparkCollect(tableSpark.count))

            // Delete all but 1 using predicates
            helper.sparkSQL(tableSpark.deleteFromHiveTableGreaterThanKey(15))
            helper.sparkSQL(tableSpark.deleteFromHiveTableLesserThanKey(15))
            expectedRows = 1
            helper.compareResult(expectedRows.toString, helper.sparkCollect(tableSpark.count))

            // No OP Delete
            helper.sparkCollect(tableSpark.deleteFromHiveTableGreaterThanKey(20))
            helper.compareResult(expectedRows.toString, helper.sparkCollect(tableSpark.count))
          } else {
            intercept[RuntimeException] {
              helper.recreate(tableSpark)
              // delete 1 row
              helper.sparkSQL(tableSpark.deleteFromHiveTableKey(11))
            }
          }
        }
        helper.myRun(testName, code)
      }
    }
  }
}
