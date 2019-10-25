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


import org.apache.commons.logging.LogFactory
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.util._
import org.scalatest._

import scala.util.control.NonFatal

class WriteSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  val log = LogManager.getLogger(this.getClass)
  log.setLevel(Level.INFO)

  var helper: TestHelper = _;
  val isDebug = true

  val DEFAULT_DBNAME =  "HiveTestDB"
  val defaultPred = " intCol < 5 "
  val cols = Map(
    ("intCol","int"),
    ("doubleCol","double"),
    ("floatCol","float"),
    ("booleanCol","boolean")
    //   ("dateCol","date")
  )

  override def beforeAll() {
    try {

      helper = new TestHelper();
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


  // Test Run
  insertIntoOverwriteTestForFullAcidTables(Table.allFullAcidTypes())
  insertIntoOverwriteTestForInsertOnlyTables(Table.allInsertOnlyTypes())

  // Insert Into/Overwrite test for full acid tables
  def insertIntoOverwriteTestForFullAcidTables(tTypes: List[(String,Boolean)]): Unit = {
    tTypes.foreach { case (tType, isPartitioned) =>
      val tableNameHive = "tHive"
      val tableNameSpark = "tSpark"
      val testName = s"Simple InsertInto Test for $tableNameHive/$tableNameSpark type $tType"
      test(testName) {
        val tableHive = new Table(DEFAULT_DBNAME, tableNameHive, cols, tType, isPartitioned)
        val tableSpark = new Table(DEFAULT_DBNAME, tableNameSpark, cols, tType, isPartitioned)
        def code() = {
          helper.recreate(tableHive)
          helper.recreate(tableSpark)

          // Insert into rows in both tables from Hive and Spark
          helper.hiveExecute(tableHive.insertIntoHiveTableKeyRange(11, 20))
          helper.sparkSQL(tableSpark.insertIntoSparkTableKeyRange(11, 20))
          var expectedRows = 10
          helper.compareTwoTablesViaHive(tableHive, tableSpark, "After Insert Into", expectedRows)
          helper.compareTwoTablesViaSpark(tableHive, tableSpark, "After Insert Into", expectedRows)

          // Insert overwrite rows in both tables from Hive and Spark
          helper.hiveExecute(tableHive.insertOverwriteHiveTableKeyRange(16, 25))
          helper.sparkSQL(tableSpark.insertOverwriteSparkTableKeyRange(16, 25))
          expectedRows = if (tableHive.isPartitioned) 15 else 10
          helper.compareTwoTablesViaHive(tableHive, tableSpark, "After Insert Overwrite", expectedRows)
          helper.compareTwoTablesViaSpark(tableHive, tableSpark, "After Insert Overwrite", expectedRows)

          // Insert overwrite rows in both tables - add rows in hive table from spark and vice versa
          helper.hiveExecute(tableSpark.insertOverwriteHiveTableKeyRange(24, 27))
          helper.sparkSQL(tableHive.insertOverwriteSparkTableKeyRange(24, 27))
          expectedRows = if (tableHive.isPartitioned) expectedRows + 2 else 4
          helper.compareTwoTablesViaHive(tableHive, tableSpark, "After Insert Overwrite", expectedRows)
          helper.compareTwoTablesViaSpark(tableHive, tableSpark, "After Insert Overwrite", expectedRows)

          // Insert into rows in both tables - add rows in hive table from spark and vice versa
          helper.hiveExecute(tableSpark.insertIntoHiveTableKeyRange(24, 27))
          helper.sparkSQL(tableHive.insertIntoSparkTableKeyRange(24, 27))
          expectedRows = expectedRows + 4
          helper.compareTwoTablesViaHive(tableHive, tableSpark, "After Insert Into", expectedRows)
          helper.compareTwoTablesViaSpark(tableHive, tableSpark, "After Insert Into", expectedRows)

        }
        helper.myRun(testName, code)
      }
    }
  }

  def insertIntoOverwriteTestForInsertOnlyTables(tTypes: List[(String,Boolean)]): Unit = {
    tTypes.foreach { case (tType, isPartitioned) =>
      val tableNameSpark = "tSpark"
      val testName = s"Simple InsertInto Test for $tableNameSpark type $tType"
      test(testName) {
        val tableSpark = new Table(DEFAULT_DBNAME, tableNameSpark, cols, tType, isPartitioned)
        def code() = {
          helper.recreate(tableSpark)

          val exception1 = intercept[RuntimeException] {
            helper.sparkSQL(tableSpark.insertIntoSparkTableKeyRange(11, 20))
          }
          assert(exception1.getMessage.contains(
            "Unsupported operation type - INSERT_INTO for InsertOnly tables"))

          val exception2 = intercept[RuntimeException] {
            helper.sparkSQL(tableSpark.insertOverwriteSparkTableKeyRange(16, 25))
          }
          assert(exception2.getMessage.contains(
            "Unsupported operation type - INSERT_OVERWRITE for InsertOnly tables"))
        }
        helper.myRun(testName, code)
      }
    }
  }

}
