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

package com.qubole.spark.datasources.hiveacid

import java.io.StringWriter
import java.net.URLClassLoader
import java.net.URL
import java.sql.ResultSet

import org.apache.commons.logging.LogFactory
import org.apache.log4j.{LogManager, Level}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.util._

import org.scalatest._

import scala.util.control.NonFatal

import org.apache.spark.sql.functions.col

class HiveACIDSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  val log = LogManager.getLogger(this.getClass)
  log.setLevel(Level.INFO)

  var spark : SparkSession = _
  var hiveClient : TestHiveClient = _

  val DEFAULT_DBNAME =  "HiveTestDB"

  val cols = Map(
    ("intCol","int"),
    ("doubleCol","double"),
    ("floatCol","float"),
    ("booleanCol","boolean")
 //   ("dateCol","date")
  )

  override def beforeAll() {
    try {
      // Clients
      spark = TestSparkSession.getSession()
      hiveClient = new TestHiveClient()
      // DB
      hiveClient.execute("DROP DATABASE IF EXISTS "+ DEFAULT_DBNAME +" CASCADE", true)
      hiveClient.execute("CREATE DATABASE "+ DEFAULT_DBNAME, true)
    } catch {
      case NonFatal(e) => log.info("failed " + e)
    }
  }

  override protected def afterAll(): Unit = {
    hiveClient.teardown()
    spark.stop()
  }

  //test("lol") {
  //  Helper.getJarsForClass("org/apache/hadoop/hive/ql/io/orc/OrcSerde.class")
  //  Helper.getJarsForClass("org/apache/hadoop/hive/serde2/avro/AvroSerde.class")
  //}

  //mergeTest(List((Table.orcFullACIDTable, false)))

  // Test runs
  //
  // e.g joinTest(((orcFullACIDTable, orcPartitionedInsertOnlyTable)
  //joinTest(Table.allFullAcidTypes(), Table.allFullAcidTypes())
  //joinTest(Table.allInsertOnlyTypes(), Table.allFullAcidTypes())
  //joinTest(Table.allInsertOnlyTypes(), Table.allInsertOnlyTypes())

  //
  // e.g compactionTest(((orcFullACIDTable,false)))
  //compactionTest(Table.allFullAcidTypes())
  //compactionWithInsertTest(Table.allFullAcidTypes())
  //compactionWithInsertTest(Table.allInsertOnlyTypes())

  //
  // e.g simpleReadTest(((orcFullACIDTable, false), (orcPartitionedInsertOnlyTable, true)))
  // predicateReadTest(List((Table.orcFullACIDTable, false)))
  // simpleReadTest(Table.allFullAcidTypes())
  //simpleReadTest(Table.allInsertOnlyTypes())

  nonAcidToAcidConversionTest(Table.allNonAcidTypes())

  // Merge Test
  //
  // 1. Disable comaction on the table.
  // 2. Insert bunch of rows into the table.
  // 3. Update the table conditional to create "Merged Table"
  // 4. Read entire table using hive client
  // 5. Read entire table using sparkSQL
  // 6. Read entire table using spark dataframe API
  // Verify: Both spark reads are same as hive read
  def mergeTest(tTypes: List[(String,Boolean)]): Unit = {

    tTypes.foreach { case (tType, isPartitioned) =>
      val tName = "t1"
      val testName = "Simple Merged table Test for " + tName + " type " + tType
      test("testName") {
        val table = new Table(DEFAULT_DBNAME, "t1", cols, tType, isPartitioned)
        def code(): Unit = {

          recreate(table)

          hiveClient.execute(table.disableCompaction, true)
          hiveClient.execute(table.insertIntoHiveTableKeyRange(1, 10), true)
          hiveClient.execute(table.updateByMergeHiveTable, true)

          val hiveResStr = hiveClient.executeQuery(table.hiveSelect, true)

          val (df1, df2) = sparkGetDF(table)

          Helper.compareResult(hiveResStr, df1.collect())
          Helper.compareResult(hiveResStr, df2.collect())
        }
        myRun(testName, code)
      }
    }
  }

  // Simple read test
  //
  // 1. Write bunch of rows using hive client
  // 2. Read entire table using hive client
  // 3. Read entire table using sparkSQL
  // 4. Read entire table using spark dataframe API
  // Verify: Both spark reads are same as hive read
  def simpleReadTest(tTypes: List[(String,Boolean)]): Unit = {


    tTypes.foreach { case (tType, isPartitioned) =>
      val tName = "t1"
      val testName = "Simple Read Test for " + tName + " type " + tType
      test(testName) {
        val table = new Table(DEFAULT_DBNAME, tName, cols, tType, isPartitioned)
        def code() = {

          recreate(table)

          hiveClient.execute(table.insertIntoHiveTableKeyRange(1, 1), false)
          val hiveResStr = hiveClient.executeQuery(table.hiveSelect, true)

          val (df1, df2) = sparkGetDF(table)

          Helper.compareResult(hiveResStr, df1.collect())
          Helper.compareResult(hiveResStr, df2.collect())
        }
        myRun(testName, code)
      }
    }
  }
/*
  // Read with predicate test
  //
  // 1. Write bunch of rows using hive client
  // 2. Read entire table using hive client
  // 3. Read entire table using sparkSQL
  // 4. Read entire table using spark dataframe API
  // Verify: Both spark reads are same as hive read
  def predicateReadTest(tTypes: List[(String,Boolean)]): Unit = {

    def code(table :Table): Unit = {

      recreate(table)

      hiveClient.execute(table.insertIntoHiveTableKeyRange(1, 1), false)
      val hiveResStr = hiveClient.executeQuery(table.hiveSelectWithPred, true)

      val (df1, df2) = sparkGetDF(table)

      Helper.compareResult(hiveResStr, df1.collect())
      Helper.compareResult(hiveResStr, df2.collect())
    }

    tTypes.foreach { case (tType, isPartitioned) =>
      val tName = "t1"
      val testName = "Predicate Read Test for " + tName + " type " + tType
      test(testName) {
        val table = new Table(DEFAULT_DBNAME, tName, cols, tType, isPartitioned)
        run3(testName, table, code)
      }
    }
  }

  // Read with projection test
  //
  // 1. Write bunch of rows using hive client
  // 2. Read entire table using hive client
  // 3. Read entire table using sparkSQL
  // 4. Read entire table using spark dataframe API
  // Verify: Both spark reads are same as hive read
  def projReadTest(tTypes: List[(String,Boolean)]): Unit = {

    def code(table :Table): Unit = {

      recreate(table)

      hiveClient.execute(table.insertIntoHiveTableKeyRange(1, 1), false)
      val hiveResStr = hiveClient.executeQuery(table.hiveSelectWithPred, true)

      val (df1, df2) = sparkGetDF(table)

      Helper.compareResult(hiveResStr, df1.collect())
      Helper.compareResult(hiveResStr, df2.collect())
    }

    tTypes.foreach { case (tType, isPartitioned) =>
      val tName = "t1"
      val testName = "Predicate Read Test for " + tName + " type " + tType
      test(testName) {
        val table = new Table(DEFAULT_DBNAME, tName, cols, tType, isPartitioned)
        run3(testName, table, code)
      }
    }
  }
*/


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
  def compactionTest(tTypes: List[(String,Boolean)]): Unit = {

    tTypes.foreach { case (tType, isPartitioned) =>
      val tName = "t1"
      val testName = "Simple Compaction Test for " + tName + " type " + tType
      test(testName) {
        val table = new Table(DEFAULT_DBNAME, tName, cols, tType, isPartitioned)
        def code() = {

          recreate(table)

          hiveClient.execute(table.disableCompaction, true)
          hiveClient.execute(table.insertIntoHiveTableKeyRange(1, 10), true)

          val hiveResStr = hiveClient.executeQuery(table.hiveSelect, true)

          val (df1, df2) = sparkGetDF(table)

          // Materialize it once
          Helper.compareResult(hiveResStr, df1.collect())
          Helper.compareResult(hiveResStr, df2.collect())

          hiveClient.execute(table.deleteFromHiveTableKey(3), true)
          hiveClient.execute(table.deleteFromHiveTableKey(4), true)
          hiveClient.execute(table.deleteFromHiveTableKey(5), true)
          hiveClient.execute(table.deleteFromHiveTableKey(6), true)

          Helper.compareResult(hiveResStr, df1.collect())
          Helper.compareResult(hiveResStr, df2.collect())

          hiveClient.execute(table.minorCompaction, true)

          Helper.compareResult(hiveResStr, df1.collect())
          Helper.compareResult(hiveResStr, df2.collect())

          hiveClient.execute(table.majorCompaction, true)

          Helper.compareResult(hiveResStr, df1.collect())
          Helper.compareResult(hiveResStr, df2.collect())

          hiveClient.execute(table.updateInHiveTableKey(7), true)
          hiveClient.execute(table.updateInHiveTableKey(8), true)
          hiveClient.execute(table.updateInHiveTableKey(9), true)
          hiveClient.execute(table.updateInHiveTableKey(10), true)

          hiveClient.execute(table.minorCompaction, true)

          Helper.compareResult(hiveResStr, df1.collect())
          Helper.compareResult(hiveResStr, df2.collect())

          hiveClient.execute(table.majorCompaction, true)

          Helper.compareResult(hiveResStr, df1.collect())
          Helper.compareResult(hiveResStr, df2.collect())

        }
        myRun(testName, code)
      }
    }
  }

  // Compaction test with inserts
  //
  // 1. Disable comaction on the table.
  // 2. Insert bunch of rows into the table.
  // 4. Read entire table using hive client
  // 5. Insert bunch of rows, this would to bunch of new inserts.
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
  def compactionWithInsertTest(tTypes: List[(String,Boolean)]): Unit = {

    tTypes.foreach { case (tType, isPartitioned) =>
      val tName = "t1"
      val testName = "Simple Compaction With Insert Test for " + tName + " type " + tType
      test(testName) {
        val table = new Table(DEFAULT_DBNAME, tName, cols, tType, isPartitioned)
        def code() = {

          recreate(table)

          hiveClient.execute(table.disableCompaction, true)
          hiveClient.execute(table.insertIntoHiveTableKeyRange(1, 10), true)

          val hiveResStr = hiveClient.executeQuery(table.hiveSelect, true)

          val (df1, df2) = sparkGetDF(table)

          // Materialize it once
          Helper.compareResult(hiveResStr, df1.collect())
          Helper.compareResult(hiveResStr, df2.collect())

          hiveClient.execute(table.insertIntoHiveTableKey(11), true)
          hiveClient.execute(table.insertIntoHiveTableKey(12), true)
          hiveClient.execute(table.insertIntoHiveTableKey(13), true)
          hiveClient.execute(table.insertIntoHiveTableKey(14), true)
          hiveClient.execute(table.insertIntoHiveTableKey(15), true)

          Helper.compareResult(hiveResStr, df1.collect())
          Helper.compareResult(hiveResStr, df2.collect())

          hiveClient.execute(table.minorCompaction, true)

          Helper.compareResult(hiveResStr, df1.collect())
          Helper.compareResult(hiveResStr, df2.collect())

          hiveClient.execute(table.majorCompaction, true)

          Helper.compareResult(hiveResStr, df1.collect())
          Helper.compareResult(hiveResStr, df2.collect())
        }
        myRun(testName, code)
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

            recreate(table1)
            recreate(table2)

            hiveClient.execute(table1.insertIntoHiveTableKeyRange(1, 15), true)
            hiveClient.execute(table2.insertIntoHiveTableKeyRange(10, 25), true)

            var hiveResStr = hiveClient.executeQuery(Table.hiveJoin(table1, table2), true)
            val sparkRes1 = sparkCollect(Table.sparkJoin(table1, table2), true)
            Helper.compareResult(hiveResStr, sparkRes1)
          }

          myRun(testName, code)
        }
      }
    }
  }

  def nonAcidToAcidConversionTest(tTypes: List[(String,Boolean)]): Unit = {
    tTypes.foreach { case (tType, isPartitioned) =>
      val tName = "t1"
      val testName = "NonAcid to Acid conversion test for " + tName + " type " + tType
      test(testName) {
        println(s"--------------------- $testName --------------------------")
        val table = new Table(DEFAULT_DBNAME, tName, cols, tType, isPartitioned)
        def code() = {
          recreate(table, false)
          hiveClient.execute(table.insertIntoHiveTableKeyRange(1, 1), false)
          val hiveResStr = hiveClient.executeQuery(table.hiveSelect, true)

          // Convert to full acid table
          hiveClient.execute(table.alterToTransactionalFullAcidTable, true)
          sparkCollect(table.sparkCreate, true)

          // Check results from Spark
          val (dfFromSql, dfFromScala) = sparkGetDF(table)
          Helper.compareResult(hiveResStr, dfFromSql.collect())
          Helper.compareResult(hiveResStr, dfFromScala.collect())


          // Insert more rows in the table from Hive and compare result from Hive and Spark
          hiveClient.execute(table.insertIntoHiveTableKeyRange(10, 20), true)
          val hiveResStr1 = hiveClient.executeQuery(table.hiveSelect, true)
          val (dfFromSql1, dfFromScala1) = sparkGetDF(table)
          Helper.compareResult(hiveResStr1, dfFromSql1.collect())
          Helper.compareResult(hiveResStr1, dfFromScala1.collect())


          // Update some rows in the table from Hive and compare result from Hive and Spark
          hiveClient.execute(table.deleteFromHiveTableKey(12), true)
          hiveClient.execute(table.deleteFromHiveTableKey(18), true)
          val hiveResStr2 = hiveClient.executeQuery(table.hiveSelect, true)
          val (dfFromSql2, dfFromScala2) = sparkGetDF(table)
          Helper.compareResult(hiveResStr2, dfFromSql2.collect())
          Helper.compareResult(hiveResStr2, dfFromScala2.collect())
        }
        myRun(testName, code);
      }
    }
  }

  /*
   * Utility functions
   */

  /*
  private def sparkGetDFWithProj(table: Table): (DataFrame, DataFrame) = {
    val df1 = spark.read.format("HiveAcid").options(Map("table" -> table.hiveTname)).select(table.sparkDFProj)
    df1 = totalOrderBy(table, df1)
    val df2 = sparkSQL(table.sparkSelectWithProj, true)
    return (df1, df2)
  }

  private def sparkGetDFWithPred(table: Table): (DataFrame, DataFrame) = {
    val df1 = spark.read.format("HiveAcid").options(Map("table" -> table.hiveTname)).filter($table.sparkDFPred)
    df1 = totalOrderBy(table, df1)
    val df2 = sparkSQL(table.sparkSelectWithPred, true)
    return (df1, df2)
  }
  */

  private def sparkGetDF(table: Table): (DataFrame, DataFrame) = {
    var df1 = spark.read.format("HiveAcid").options(Map("table" -> table.hiveTname)).load()
    df1 = totalOrderBy(table, df1)

    val df2 = sparkSQL(table.sparkSelect, true)
    return (df1, df2)
  }

  private def sparkSQL(cmd: String, verbose: Boolean): DataFrame = {
    if (verbose) log.info(s"\n\nSparkSQL> ${cmd}\n")
    spark.sql(cmd)
  }

  private def sparkCollect(cmd: String, verbose: Boolean): Array[Row] = {
    if (verbose) log.info(s"\n\nSparkSQL> ${cmd}\n")
    spark.sql(cmd).collect()
  }

  private def totalOrderBy(table: Table, df: DataFrame): DataFrame = {
    var df1 = df
    table.getColMap.foreach{case(colName, _) => df1 = df1.orderBy(col(colName))}
    df1
  }

  private def recreate(table: Table, createSymlinkSparkTables: Boolean = true): Unit = {
    sparkCollect(table.sparkDrop, false)
    hiveClient.execute(table.hiveDrop, false)
    hiveClient.execute(table.hiveCreate, true)
    if (createSymlinkSparkTables) {
      sparkCollect(table.sparkCreate, false)
    }
  }

  // Run single type run
  @throws(classOf[Exception])
  private def myRun(testName: String, code: () => Unit): Unit = {
    try {
      log.info(s">>>>>>>>>>>>>>>>>>> $testName")
      code()
    } catch {
      case NonFatal(e) =>
        log.info(s"Failed test[${testName}]:$e")
        throw e
    }
  }

}

object Helper {

  val log = LogManager.getLogger(this.getClass)
  log.setLevel(Level.INFO)
  // Convert Array of Spark Rows into a String
  private def sparkRowsToStr(rows: Array[Row]): String = {
    rows.map(row => row.mkString(",")).mkString("\n")
  }

  // Compare the results
  def compareResult(hiveResStr: String, sparkRes: Array[Row]): Unit = {
    val sparkResStr = sparkRowsToStr(sparkRes)
    log.info(s"Comparing hive: $hiveResStr \n Spark: $sparkResStr")
    assert(hiveResStr == sparkResStr)
  }

  // Given a className, identify all the jars in classpath that contains the class
  def getJarsForClass(className: String): Unit = {
    def list_urls(cl: ClassLoader): Array[java.net.URL] = cl match {
      case null => Array()
      case u: java.net.URLClassLoader => u.getURLs() ++ list_urls(cl.getParent)
      case _ => list_urls(cl.getParent)
    }

    def findJarsHavingClass(name: String, jarList: Array[URL]): Array[URL] = {
      var resultJarsArray = Array[URL]()
      for( x <- jarList ){
        val ucl = new URLClassLoader(Array(x), ClassLoader.getSystemClassLoader.getParent.getParent)
        val classPath = ucl.findResource(name)
        if (classPath != null) {
          resultJarsArray = resultJarsArray :+ classPath
        }
      }
      return resultJarsArray
    }

    val allJars = list_urls(getClass.getClassLoader).distinct
    val requiredJars = findJarsHavingClass(className, allJars)

    log.info(s"Class: $className found in following ${requiredJars.size} jars:")
    requiredJars.foreach(uri => log.info(uri.toString))
  }
}
