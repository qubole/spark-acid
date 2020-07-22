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

package com.qubole.spark.hiveacid

import org.apache.log4j.{Level, LogManager, Logger}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import scala.util.control.NonFatal

class MergeSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  val log: Logger = LogManager.getLogger(this.getClass)
  log.setLevel(Level.INFO)

  var helper: TestHelper = _
  val isDebug = true

  val DEFAULT_DBNAME =  "HiveTestMergeDB"
  val cols: Map[String, String] = Map(
    ("intCol","int"),
    ("doubleCol","double"),
    ("floatCol","float"),
    ("booleanCol","boolean")
  )
  val sourcePartitioned = new Table(DEFAULT_DBNAME, "sourceTablePartitioned",
    cols, Table.orcPartitionedFullACIDTable, true)
  val sourceTable = new Table(DEFAULT_DBNAME, "sourceTableNonPartitioned",
    cols, Table.orcFullACIDTable, false)
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
      helper.recreate(sourcePartitioned)
      helper.recreate(sourceTable)
      helper.hiveExecute(sourcePartitioned.insertIntoHiveTableKeyRange(11, 25))
      helper.hiveExecute(sourceTable.insertIntoHiveTableKeyRange(11, 25))
    } catch {
      case NonFatal(e) => log.info("failed " + e)
    }
  }

  override protected def afterAll(): Unit = {
    helper.hiveExecute(s"DROP TABLE IF EXISTS ${sourceTable.hiveTname}")
    helper.hiveExecute(s"DROP TABLE IF EXISTS ${sourcePartitioned.hiveTname}")
    helper.hiveExecute("DROP DATABASE IF EXISTS "+ DEFAULT_DBNAME +" CASCADE")
    helper.destroy()
  }

  test("Simple Merge Test on nonPartitioned ORC") {
    simpleMergeTestForFullAcidTables(Table.orcFullACIDTable, false, true)
  }

  test("Simple Merge Test on Partitioned ORC with TableAliasInMerge: false") {
    simpleMergeTestForFullAcidTables(Table.orcPartitionedFullACIDTable, true, true, false)
  }

  test("Merge Test on nonPartitioned ORC for conflicting match condition") {
    mergeTestWithConflictingMatch(Table.orcFullACIDTable, false)
  }

  test("Merge Test on Partitioned ORC for conflicting match condition") {
    mergeTestWithConflictingMatch(Table.orcPartitionedFullACIDTable, true)
  }

  test("Merge Test on nonPartitioned ORC with just Insert") {
    mergeTestWithJustInsert(Table.orcFullACIDTable, false)
  }

  test("Merge Test on Partitioned ORC with Just Insert") {
    mergeTestWithJustInsert(Table.orcPartitionedFullACIDTable, true)
  }

  test("Merge on original files") {
    val srcTable =  new Table(DEFAULT_DBNAME, "srcInvalid", cols, Table.orcTable, false)
    helper.recreate(srcTable, false)
    helper.hiveExecute(srcTable.insertIntoHiveTableKey(11))
    helper.hiveExecute(srcTable.alterToTransactionalFullAcidTable)
    helper.sparkCollect(srcTable
      .mergeCommand(sourceTable.hiveTname, "t.key > 12",
      "t.key = 11", "*"))
    val expectedRows = 14
    helper.compareResult(expectedRows.toString, helper.sparkCollect(srcTable.count))
  }

  test("Merging Complex Data type - INSERT and DELETE") {
    def getCreate(name: String) = s"create table $name (id int, " +
      "addr struct<AddressLine1:array<string>," +
      "City:array<string>,Country:array<string>," +
      "StateProvince:array<string>,Zip:array<string>>, prop map<int, string>) " +
      "stored as ORC tblproperties('transactional' = 'true')"

    val srcTblName = DEFAULT_DBNAME + ".srcTable"
    val tgtTblName = DEFAULT_DBNAME + ".tgtTable"
    helper.sparkCollect(s"DROP table IF EXISTS $tgtTblName")
    helper.sparkCollect(s"DROP table IF EXISTS $srcTblName")
    helper.hiveExecute(getCreate(srcTblName))
    helper.hiveExecute(getCreate(tgtTblName))
    helper.hiveExecute(s"insert into $tgtTblName values (1," +
      s"named_struct('addressLine1', array('xyz', 'abc'), 'City', array('xyz', 'abc'), 'Country', array('xyz', 'abc')," +
      s" 'StateProvince', array('xyz', 'abc'), 'Zip', array()), map(1, 'test'))")
    helper.hiveExecute(s"insert into $srcTblName values (1," +
      s"named_struct('addressLine1', array('u', 'abc'), 'City', array('xyz', 'abc'), 'Country', array('xyz', 'abc'), " +
      s"'StateProvince', array('xyz', 'abc'), 'Zip', array()), map(1, 'test'))," +
      s"(2, named_struct('addressLine1', array('u', 'abc'), 'City', array('xyz', 'abc'), 'Country', array('xyz', 'abc'), " +
      s"'StateProvince', array('xyz', 'abc'), 'Zip', array()),map(1, 'test'))")
    val merge = s"""merge into $tgtTblName t using $srcTblName s on s.id=t.id
         | when matched  then update set addr=s.addr
         | when not matched then insert values(*)""".stripMargin
    helper.sparkCollect(merge)
    helper.compareResult(
      helper.sparkCollect(s"select * from $tgtTblName order by id"),
      helper.sparkCollect(s"select * from $srcTblName order by id"))
  }

  test("Check Cardinality Validation error") {
    val srcTable =  new Table(DEFAULT_DBNAME, "srcInvalid", cols, Table.orcFullACIDTable, false)
    helper.recreate(srcTable)
    helper.hiveExecute(srcTable.insertIntoHiveTableKeys(Seq(11, 11)))

    // 2 rows of source matches 1 row of target at key = 11
    val thrown = intercept[RuntimeException] {
      helper.sparkCollect(sourceTable
        .mergeCommand(srcTable.hiveTname, "t.key >= 11 and t.key < 17",
          "t.key > 15", "*"))
      helper.hiveExecute(s"DROP TABLE IF EXISTS ${srcTable.hiveTname}")
    }
    assert(thrown.getMessage.contains("MERGE is not supported when multiple rows of " +
      "source match same target row. 1 rows in target had multiple matches. " +
      "Please check MERGE match condition and try again"))
  }

  test("Merge Test on Partitioned ORC with Update on Columns") {
    val tableNameSpark = "tSparkUpdatePartitionColumned"
    val tableSpark = new Table(DEFAULT_DBNAME, tableNameSpark, cols,
      Table.orcPartitionedFullACIDTable, true)
    helper.recreate(tableSpark, false)
    val thrown = intercept[com.qubole.spark.hiveacid.AnalysisException] {
      helper.sparkSQL(tableSpark.mergeCommand(sourcePartitioned.hiveTname, "t.key >= 11 and t.key < 17",
        "t.key > 16", "*", "intCol=s.intCol * 10, ptnCol=s.ptnCol"))
    }
    assert(thrown.getMessage().contains("UPDATE on the partition columns are not allowed"))
  }

  test("Check for Merge update on multi statements with 1 bucket") {
    /** In this test following is done:
      *  ** Insert into target table DF with statement id 1. Insert should just create one bucket file i.e., bucket0000
      *  ** Insert into target table DF with no statement id. Insert should just create one bucket file i.e., bucket0000
      *  ** Note encoded bucket id in both the above rows will be different due to difference in statement Id
      *  ** Try to update one row from each of the above transaction. It is expected that both are updated
      */
    val spark = helper.spark
    import spark.sqlContext.implicits._
    val targetTable = s"$DEFAULT_DBNAME.target_bucket1"
    val sourceTable = s"$DEFAULT_DBNAME.source_bucket1"

    helper.hiveExecute(s"create table $targetTable (i int) stored as orc tblproperties('transactional'='true')")
    val df1 = spark.sparkContext.parallelize(Seq(1, 2, 3)).toDF().repartition(1)
    val htable = HiveAcidTable.fromSparkSession(spark, targetTable)
    htable.insertInto(df1, Some(1))
    val df2 = spark.sparkContext.parallelize(Seq(4, 5, 6)).toDF().repartition(1)
    htable.insertInto(df2)
    helper.hiveExecute(s"create table $sourceTable (i int) stored as orc tblproperties('transactional' = 'true')")
    helper.sparkSQL(s"insert into $sourceTable values (1), (4)")
    helper.sparkSQL(s"Merge into $targetTable t using $sourceTable s on t.i = s.i when matched then update set i = s.i + 1")

    val res = helper.sparkCollect(s"select * from $targetTable order by i")
    val expected = s"2\n2\n3\n5\n5\n6"
    helper.compareResult(expected, res)
  }

  test("Check for Merge update on multi statements with 2 buckets") {
    /** In this test following is done:
      *  ** Insert into target table DF with statement id 1. Insert should just create two bucket file i.e., bucket0000, bucket0001
      *  ** Insert into target table DF with no statement id. Insert should just create one bucket file i.e., bucket0000, bucket0001
      *  ** Note encoded bucket id in rows of different transaction will be different due to difference in statement Id.
      *  ** Try to update all the rows from each of the above transaction. It is expected that all rows are updated.
      */
    val spark = helper.spark
    import spark.sqlContext.implicits._
    val targetTable = s"$DEFAULT_DBNAME.target_bucket2"
    val sourceTable = s"$DEFAULT_DBNAME.source_bucket2"

    helper.hiveExecute(s"create table $targetTable (i int) stored as orc tblproperties('transactional'='true')")
    val df1 = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).toDF().repartition(2)
    val htable = HiveAcidTable.fromSparkSession(spark, targetTable)
    htable.insertInto(df1, Some(1))
    val df2 = spark.sparkContext.parallelize(Seq(11, 12, 13, 14, 15, 16, 17, 18, 19, 20)).toDF().repartition(2)
    htable.insertInto(df2)
    helper.hiveExecute(s"create table $sourceTable (i int) stored as orc tblproperties('transactional' = 'true')")
    helper.sparkSQL(s"insert into $sourceTable select * from $targetTable")
    helper.sparkSQL(s"Merge into $targetTable t using $sourceTable s on t.i = s.i when matched then update set i = s.i + 1")

    val res = helper.sparkCollect(s"select * from $targetTable order by i")
    val expected = s"2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14\n15\n16\n17\n18\n19\n20\n21"
    helper.compareResult(expected, res)
  }

  // Merge test for full acid tables
  def mergeTestWithJustInsert(tType: String, isPartitioned: Boolean): Unit = {
    val tableNameSpark = if (isPartitioned) {
      "tSparkMergePartitioned"
    } else  {
      "tSparkMergeNonPart"
    }
    val testName = s"Simple Merge Test for $tableNameSpark type $tType"
    val tableSpark = new Table(DEFAULT_DBNAME, tableNameSpark, cols, tType, isPartitioned)

    def code(): Unit = {
      val srcTable = if (isPartitioned) {
        sourcePartitioned
      } else {
        sourceTable
      }
      helper.recreate(tableSpark, false)
      if (isPartitioned) {
        helper.sparkCollect(tableSpark
          .mergeCommandWithInsertOnly(srcTable.hiveTname, "*"))
      } else {
        helper.sparkCollect(tableSpark
          .mergeCommandWithInsertOnly(srcTable.hiveTname,
            srcTable.getColMap.map(x => x._1).mkString(",")))
      }

      val expectedRows = 15
      helper.compareResult(expectedRows.toString, helper.sparkCollect(tableSpark.count))
    }

    helper.myRun(testName, code)
  }

  // Merge test for full acid tables
  def mergeTestWithConflictingMatch(tType: String, isPartitioned: Boolean): Unit = {
    val tableNameSpark = if (isPartitioned) {
      "tSparkMergePartitioned"
    } else  {
      "tSparkMergeNonPart"
    }
    val testName = s"Simple Merge Test for $tableNameSpark type $tType"
    val tableSpark = new Table(DEFAULT_DBNAME, tableNameSpark, cols, tType, isPartitioned)

    def code(): Unit = {
      val srcTable = if (isPartitioned) {
        sourcePartitioned
      } else {
        sourceTable
      }
      helper.recreate(tableSpark)
      helper.hiveExecute(tableSpark.insertIntoHiveTableKeyRange(11, 20))
      val expectedUpdateValue = helper.sparkCollect(tableSpark.selectExpectedUpdateCol(15))
      // key 15,16 are conflicting
      // first is update so they will be updated
      helper.sparkCollect(tableSpark
        .mergeCommand(srcTable.hiveTname, "t.key > 14 and t.key < 17",
          "t.key > 14", "*"))
      val expectedRows = 11
      helper.compareResult(expectedRows.toString, helper.sparkCollect(tableSpark.count))
      // Verify INSERT
      helper.compareResult("5", helper.sparkCollect(tableSpark.countWithPred("key > 20")))
      // Verify DELETE on non-conflicting keys
      helper.compareResult("0",
        helper.sparkCollect(tableSpark.countWithPred("key > 16 and key < 21")))
      // Verify UPDATE on conflicting keys
      val updatedVal = helper.sparkCollect(tableSpark.selectUpdateCol(15))
      helper.compareResult(expectedUpdateValue, updatedVal)

      // 13 is conflicting and should be deleted
      helper.sparkCollect(tableSpark
        .mergeCommandWithDeleteFirst(srcTable.hiveTname, "t.key = 13",
          "t.key  = 13", "*"))
      helper.compareResult("14", helper.sparkCollect(tableSpark.count))
      // Verify DELETE on conflicting keys
      helper.compareResult("0",
        helper.sparkCollect(tableSpark.countWithPred("key = 13")))
    }

    helper.myRun(testName, code)
  }

  // Merge test for full acid tables
  def simpleMergeTestForFullAcidTables(tType: String, isPartitioned: Boolean,
                                       positiveTest: Boolean,
                                       useTableAliasInMerge: Boolean = true): Unit = {
    val tableNameSpark = if (isPartitioned) {
      "tSparkMergePartitioned"
    } else  {
      "tSparkMergeNonPart"
    }
    val testName = s"Simple Merge Test for $tableNameSpark type $tType with tableAliasInMerge: ${useTableAliasInMerge}"
    val tableSpark = new Table(DEFAULT_DBNAME, tableNameSpark, cols, tType, isPartitioned)

    def code(): Unit = {
      val srcTable = if (isPartitioned) {
        sourcePartitioned
      } else {
        sourceTable
      }
      if (positiveTest) {
        helper.recreate(tableSpark)
        helper.hiveExecute(tableSpark.insertIntoHiveTableKeyRange(11, 20))
        val expectedUpdateValue = helper.sparkCollect(tableSpark.selectExpectedUpdateCol(16))
        val mergeCommand = if (useTableAliasInMerge) {
          tableSpark
            .mergeCommand(srcTable.hiveTname, "t.key >= 11 and t.key < 17",
              "t.key > 16", "*", "intCol=s.intCol * 10")
        } else {
          tableSpark
            .mergeCommandWithoutTableAlias(srcTable.hiveTname,
              s"${tableSpark.hiveTname}.key >= 11 and ${tableSpark.hiveTname}.key < 17",
              s"${tableSpark.hiveTname}.key > 16", "*",
              s"intCol=${srcTable.hiveTname}.intCol * 10")
        }

        helper.sparkCollect(mergeCommand)
        val expectedRows = 11
        helper.compareResult(expectedRows.toString, helper.sparkCollect(tableSpark.count))
        // Verify INSERT
        helper.compareResult("5", helper.sparkCollect(tableSpark.countWithPred("key > 20")))
        // Verify DELETE
        helper.compareResult("0",
          helper.sparkCollect(tableSpark.countWithPred("key > 16 and key < 21")))
        // Verify UPDATE
        val updatedVal = helper.sparkCollect(tableSpark.selectUpdateCol(16))
        helper.compareResult(expectedUpdateValue, updatedVal)
      } else {
        helper.recreate(tableSpark, false)
        val mergeCommand = if (useTableAliasInMerge) {
          tableSpark
            .mergeCommand(srcTable.hiveTname, "t.key >= 11 and t.key < 16",
              "t.key > 15", "*", "intCol=s.intCol * 10")
        } else {
          tableSpark
            .mergeCommandWithoutTableAlias(srcTable.hiveTname,
              s"${tableSpark.hiveTname}.key >= 11 and ${tableSpark.hiveTname}.key < 16",
              s"${tableSpark.hiveTname}.key > 15", "*",
              s"intCol=${srcTable.hiveTname}.intCol * 10")
        }
        intercept[RuntimeException] {
          helper.sparkCollect(mergeCommand)
        }
      }
    }

    helper.myRun(testName, code)
  }
}
