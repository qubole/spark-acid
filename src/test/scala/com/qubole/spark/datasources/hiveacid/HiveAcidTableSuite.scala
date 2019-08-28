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

class HiveAcidTableSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

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
  insertIntoTest(Table.allFullAcidTypes, false)

  // Read test
  //
  // 1. Write bunch of rows using hive client
  // 2. Read entire table using hive client
  // Verify: Both spark reads are same as hive read
  def insertIntoTest(tTypes: List[(String,Boolean)], insertOnly: Boolean): Unit = {
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
          helper.verifyWrites(tableHive, tableSpark)
        }
        helper.myRun(testName, code)
      }
    }
  }

}
