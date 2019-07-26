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

class HiveACIDSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  val log = LogManager.getLogger(this.getClass)
  log.setLevel(Level.INFO)

  var helper: TestHelper = _;
  val isDebug = false

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
  readTest(Table.allFullAcidTypes, false)
  readTest(Table.allInsertOnlyTypes, true)

  // NB: Cannot create merged table for insert only table
  mergeTest(Table.allFullAcidTypes, false)

  joinTest(Table.allFullAcidTypes(), Table.allFullAcidTypes())
  joinTest(Table.allInsertOnlyTypes(), Table.allFullAcidTypes())
  joinTest(Table.allInsertOnlyTypes(), Table.allInsertOnlyTypes())

  compactionTest(Table.allFullAcidTypes(), false)
  compactionTest(Table.allInsertOnlyTypes(), true)

  // NB: No run for the insert only table.
  nonAcidToAcidConversionTest(Table.allNonAcidTypes(), false)

  // Run predicatePushdown test for InsertOnly/FullAcid, Partitioned/NonPartitioned tables
  // It should work in file formats which supports predicate pushdown - orc/parquet
  predicatePushdownTest(List(
    (Table.orcPartitionedInsertOnlyTable, true, true),
    (Table.parquetPartitionedInsertOnlyTable, true, true),
    (Table.textPartitionedInsertOnlyTable, true, false),
    (Table.orcInsertOnlyTable, false, true),
    (Table.parquetInsertOnlyTable, false, true),
    (Table.textInsertOnlyTable, false, false),
     (Table.orcFullACIDTable, false, true),
    (Table.orcPartitionedFullACIDTable, true, true)
  ))


  // Read test
  //
  // 1. Write bunch of rows using hive client
  // 2. Read entire table using hive client
  // Verify: Both spark reads are same as hive read
  def readTest(tTypes: List[(String,Boolean)], insertOnly: Boolean): Unit = {
    tTypes.foreach { case (tType, isPartitioned) =>
      val tName = "t1"
      val testName = "Simple Read Test for " + tName + " type " + tType
      test(testName) {
        val table = new Table(DEFAULT_DBNAME, tName, cols, tType, isPartitioned)
        def code() = {
          helper.recreate(table)
          helper.hiveExecute(table.insertIntoHiveTableKeyRange(1, 10))
          helper.verify(table, insertOnly)
        }
        helper.myRun(testName, code)
      }
    }
  }

  def predicatePushdownTest(tTypes: List[(String,Boolean,Boolean)]): Unit = {
    tTypes.foreach { case (tType, isPartitioned, pushdownExpected) =>
      val tName = "t1"
      val testName = "Predicate pushdown test " + tName + " type " + tType
      test(testName) {
        val table = new Table(DEFAULT_DBNAME, tName, cols, tType, isPartitioned)

        def checkOutputRowsInLeafNode(df: DataFrame): Long = {
          val tableScanNode = df.queryExecution.executedPlan.collectLeaves()(0)
          val metricsMap = tableScanNode.metrics
          val dfRowsRead = metricsMap("numOutputRows").value
          log.info(s"dfRowsRead: $dfRowsRead")
          return dfRowsRead
        }

        def code() = {
          helper.withSQLConf("spark.sql.acidDs.enablePredicatePushdown" -> "true") {
            helper.recreate(table, true)
            // Inserting 5 rows in different hive queries so that we will have 5 files - one for each row
            (3 to 7).toSeq.foreach(k => helper.hiveExecute(table.insertIntoHiveTableKey(k)))

            val dfFromSql = helper.sparkSQL(table.sparkSelectWithPred(defaultPred))
            val hiveResStr = helper.hiveExecuteQuery(table.hiveSelectWithPred(defaultPred))
            helper.compareResult(hiveResStr, dfFromSql.collect())
            if (pushdownExpected) {
              assert(checkOutputRowsInLeafNode(dfFromSql) == 2L * 2)
            } else {
              assert(checkOutputRowsInLeafNode(dfFromSql) == 2L * 5)
            }

            // This query is failing in Orc and needs to be fixed
            // sparkSQL("select count(*) FROM HiveTestDB.spark_t1 t1 where intCol < 5").collect()

            // Disable the pushdown
            helper.sparkSQL("set spark.sql.acidDs.enablePredicatePushdown=false")
            val dfFromSql1 = helper.sparkSQL(table.sparkSelectWithPred(defaultPred))
            helper.compareResult(hiveResStr, dfFromSql1.collect())
            assert(checkOutputRowsInLeafNode(dfFromSql1) == 2L * 5)
          }
        }

        helper.myRun(testName, code)
      }
    }
  }

  // Merged Table Read Test
  //
  // 1. Disable comaction on the table.
  // 2. Insert bunch of rows into the table.
  // 3. Update the table conditional to create "Merged Table"
  // 4. Read entire table using hive client with and without predicate and projection
  // VERIFY: Both spark reads are same as hive read with predicate and projection
  def mergeTest(tTypes: List[(String,Boolean)], insertOnly: Boolean): Unit = {

    tTypes.foreach { case (tType, isPartitioned) =>
      val tName = "t1"
      val testName = "Simple Merged table Test for " + tName + " type " + tType

      test(testName) {
        val table = new Table(DEFAULT_DBNAME, "t1", cols, tType, isPartitioned)
        def code(): Unit = {
          helper.recreate(table)
          helper.hiveExecute(table.disableCompaction)
          helper.hiveExecute(table.insertIntoHiveTableKeyRange(1, 10))
          helper.hiveExecute(table.updateByMergeHiveTable)
          helper.verifyWithPred(table, insertOnly, defaultPred)
        }
        helper.myRun(testName, code)
      }
    }
  }

  // Non Acid To Acid Table Read test
  //
  // 1. Create a non acid table in hive
  // 2. Insert bunch of rows into the table
  // 3. Read entire table using hive client
  // 4. Alter table and convert the table into ACID table.
  // 5. Create spark sym link table over the hive table.
  // VERIFY: Both spark reads are same as hive read
  def nonAcidToAcidConversionTest(tTypes: List[(String,Boolean)], insertOnly: Boolean): Unit = {
    tTypes.foreach { case (tType, isPartitioned) =>
      val tName = "t1"
      val testName = "NonAcid to Acid conversion test for " + tName + " type " + tType
      test(testName) {
        val table = new Table(DEFAULT_DBNAME, tName, cols, tType, isPartitioned)
        def code() = {
          helper.recreate(table, false)
          helper.hiveExecute(table.insertIntoHiveTableKeyRange(1, 10))
          val hiveResStr = helper.hiveExecuteQuery(table.hiveSelect)

          // Convert to full acid table
          helper.hiveExecute(table.alterToTransactionalFullAcidTable)
          helper.sparkCollect(table.sparkCreate)

          // Special case of comparing result read before conversion
          // and after conversion.
          log.info("++ Compare result across conversion")
          val (dfFromSql, dfFromScala) = helper.sparkGetDF(table)
          helper.compareResult(hiveResStr, dfFromSql.collect())
          helper.compareResult(hiveResStr, dfFromScala.collect())

          helper.verify(table, insertOnly)
        }
        helper.myRun(testName, code)
      }
    }
  }

  // Compaction Test
  //
  // 1. Disable comaction on the table.
  // 2. Insert bunch of rows into the table.
  // 4. Read entire table using hive client
  // 5. Delete few keys, to create delete delta files.
  //
  // Check 1
  // 5. Read entire table using sparkSQL
  // 6. Read entire table using spark dataframe API
  // Verify: Both spark reads are same as hive read
  //
  // Check 2
  // 7. Trigger `Minor Compaction` and wait for it to finish.
  // 8. Read entire table using sparkSQL
  // 9. Read entire table using spark dataframe API
  // Verify: Both spark reads are same as hive read
  //
  // Check 2
  // 10. Trigger `Major Compaction` and wait for it to finish.
  // 11. Read entire table using sparkSQL
  // 12. Read entire table using spark dataframe API
  // Verify: Both spark reads are same as hive read
  def compactionTest(tTypes: List[(String,Boolean)], insertOnly: Boolean): Unit = {

    tTypes.foreach { case (tType, isPartitioned) =>
      val tName = "t1"
      val testName = "Simple Compaction Test for " + tName + " type " + tType
      test(testName) {
        val table = new Table(DEFAULT_DBNAME, tName, cols, tType, isPartitioned)
        def code() = {

          helper.recreate(table)

          helper.hiveExecute(table.disableCompaction)
          helper.hiveExecute(table.insertIntoHiveTableKeyRange(1, 3))

          val hiveResStr = helper.hiveExecuteQuery(table.hiveSelect)

          val (df1, df2) = helper.sparkGetDF(table)

          // Materialize it once
          helper.compareResult(hiveResStr, df1.collect())
          helper.compareResult(hiveResStr, df2.collect())

          helper.hiveExecute(table.insertIntoHiveTableKey(11))
          helper.hiveExecute(table.insertIntoHiveTableKey(12))
          helper.hiveExecute(table.insertIntoHiveTableKey(13))
          helper.hiveExecute(table.insertIntoHiveTableKey(14))
          helper.hiveExecute(table.insertIntoHiveTableKey(15))
          compactAndTest(hiveResStr, df1, df2)

          // Shortcut for insert Only
          if (! insertOnly) {
            helper.hiveExecute(table.deleteFromHiveTableKey(3))
            helper.hiveExecute(table.deleteFromHiveTableKey(4))
            helper.hiveExecute(table.deleteFromHiveTableKey(5))
            helper.hiveExecute(table.deleteFromHiveTableKey(6))
            compactAndTest(hiveResStr, df1, df2)

            helper.hiveExecute(table.updateInHiveTableKey(7))
            helper.hiveExecute(table.updateInHiveTableKey(8))
            helper.hiveExecute(table.updateInHiveTableKey(9))
            helper.hiveExecute(table.updateInHiveTableKey(10))
            compactAndTest(hiveResStr, df1, df2)
          }
        }

        def compactAndTest(hiveResStr: String, df1: DataFrame, df2: DataFrame) = {
          helper.compareResult(hiveResStr, df1.collect())
          helper.compareResult(hiveResStr, df2.collect())
          helper.hiveExecute(table.minorCompaction)
          helper.compareResult(hiveResStr, df1.collect())
          helper.compareResult(hiveResStr, df2.collect())
          helper.hiveExecute(table.majorCompaction)
          helper.compareResult(hiveResStr, df1.collect())
          helper.compareResult(hiveResStr, df2.collect())
        }

        helper.myRun(testName, code)
      }
    }
  }

  // Join test
  //
  // 1. Insert bunch of rows into table t1 and t2
  // 2. Perform inner join on primary key between t1 and t2 using hive client
  // 4. Perform inner join on primary key between t1 and t2 using spark sql
  // Verify: spark read is same as hive read
  def joinTest(tTypes1: List[(String,Boolean)], tTypes2: List[(String,Boolean)]): Unit = {

    tTypes1.foreach { case (tType1, isPartitioned1) =>
      val tName1 = "t1"
      val tName2 = "t2"
      tTypes2.foreach { case (tType2, isPartitioned2) =>
        val testName = "Simple Join Test for " + tName1 + " type " + tType1 + " and " + tName2 + " type " + tType2
        test(testName) {
          val table1 = new Table(DEFAULT_DBNAME, tName1, cols, tType1, isPartitioned1)
          val table2 = new Table(DEFAULT_DBNAME, tName2, cols, tType2, isPartitioned2)
          def code() = {

            helper.recreate(table1)
            helper.recreate(table2)

            helper.hiveExecute(table1.insertIntoHiveTableKeyRange(1, 15))
            helper.hiveExecute(table2.insertIntoHiveTableKeyRange(10, 25))

            var hiveResStr = helper.hiveExecuteQuery(Table.hiveJoin(table1, table2))
            val sparkRes1 = helper.sparkCollect(Table.sparkJoin(table1, table2))
            helper.compareResult(hiveResStr, sparkRes1)
          }

          helper.myRun(testName, code)
        }
      }
    }
  }
}
