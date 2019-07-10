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

  var verbose = false

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
      if (verbose) {
        log.setLevel(Level.DEBUG)
      }
      hiveClient = new TestHiveClient(verbose)
      // DB
      hiveClient.execute("DROP DATABASE IF EXISTS "+ DEFAULT_DBNAME +" CASCADE")
      hiveClient.execute("CREATE DATABASE "+ DEFAULT_DBNAME)
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

  // Test Run
  readTest(Table.allFullAcidTypes, false)
  readTest(Table.allInsertOnlyTypes, true)

  //mergeTest(Table.allFullAcidTypes, false)
  //mergeTest(Table.allInsertOnlyTypes, true)

  // e.g joinTest(((orcFullACIDTable, orcPartitionedInsertOnlyTable)
  joinTest(Table.allFullAcidTypes(), Table.allFullAcidTypes())
  joinTest(Table.allInsertOnlyTypes(), Table.allFullAcidTypes())
  joinTest(Table.allInsertOnlyTypes(), Table.allInsertOnlyTypes())

  // e.g compactionTest(((orcFullACIDTable,false)), false)
  compactionTest(Table.allFullAcidTypes(), false)
  compactionTest(Table.allInsertOnlyTypes(), true)

  // NB: No run for the insert only table.
  nonAcidToAcidConversionTest(Table.allNonAcidTypes(), false)

  // Read test
  //
  // 1. Write bunch of rows using hive client
  // 2. Read entire table using hive client with and without predicate and projection
  // Verify: Both spark reads are same as hive read
  def readTest(tTypes: List[(String,Boolean)], insertOnly: Boolean): Unit = {
    tTypes.foreach { case (tType, isPartitioned) =>
      val tName = "t1"
      val testName = "Simple Read Test for " + tName + " type " + tType
      test(testName) {
        val table = new Table(DEFAULT_DBNAME, tName, cols, tType, isPartitioned)
        def code() = {
          recreate(table)
          hiveClient.execute(table.insertIntoHiveTableKeyRange(1, 10))
          verifyAll(table, insertOnly)
        }
        myRun(testName, code)
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

      test("testName") {
        val table = new Table(DEFAULT_DBNAME, "t1", cols, tType, isPartitioned)
        def code(): Unit = {
          recreate(table)
          hiveClient.execute(table.disableCompaction)
          hiveClient.execute(table.insertIntoHiveTableKeyRange(1, 10))
          hiveClient.execute(table.updateByMergeHiveTable)
          verifyAll(table, insertOnly)
        }
        myRun(testName, code)
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
        println(s"--------------------- $testName --------------------------")
        val table = new Table(DEFAULT_DBNAME, tName, cols, tType, isPartitioned)
        def code() = {
          recreate(table, false)
          hiveClient.execute(table.insertIntoHiveTableKeyRange(1, 10))
          val hiveResStr = hiveClient.executeQuery(table.hiveSelect)

          // Convert to full acid table
          hiveClient.execute(table.alterToTransactionalFullAcidTable)
          sparkCollect(table.sparkCreate)

          // Special case of comparing result read before conversion
          // and after conversion.
          log.info("++ Compare result across conversion")
          val (dfFromSql, dfFromScala) = sparkGetDF(table)
          compareResult(hiveResStr, dfFromSql.collect())
          compareResult(hiveResStr, dfFromScala.collect())

          verify(table, insertOnly)
        }
        myRun(testName, code)
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

          recreate(table)

          hiveClient.execute(table.disableCompaction)
          hiveClient.execute(table.insertIntoHiveTableKeyRange(1, 10))

          val hiveResStr = hiveClient.executeQuery(table.hiveSelect)

          val (df1, df2) = sparkGetDF(table)

          // Materialize it once
          compareResult(hiveResStr, df1.collect())
          compareResult(hiveResStr, df2.collect())

          hiveClient.execute(table.insertIntoHiveTableKey(11))
          hiveClient.execute(table.insertIntoHiveTableKey(12))
          hiveClient.execute(table.insertIntoHiveTableKey(13))
          hiveClient.execute(table.insertIntoHiveTableKey(14))
          hiveClient.execute(table.insertIntoHiveTableKey(15))
          compactAndTest(hiveResStr, df1, df2)

          // Shortcut for insert Only
          if (! insertOnly) {
            hiveClient.execute(table.deleteFromHiveTableKey(3))
            hiveClient.execute(table.deleteFromHiveTableKey(4))
            hiveClient.execute(table.deleteFromHiveTableKey(5))
            hiveClient.execute(table.deleteFromHiveTableKey(6))
            compactAndTest(hiveResStr, df1, df2)

            hiveClient.execute(table.updateInHiveTableKey(7))
            hiveClient.execute(table.updateInHiveTableKey(8))
            hiveClient.execute(table.updateInHiveTableKey(9))
            hiveClient.execute(table.updateInHiveTableKey(10))
            compactAndTest(hiveResStr, df1, df2)
          }
        }

        def compactAndTest(hiveResStr: String, df1: DataFrame, df2: DataFrame) = {
          compareResult(hiveResStr, df1.collect())
          compareResult(hiveResStr, df2.collect())
          hiveClient.execute(table.minorCompaction)
          compareResult(hiveResStr, df1.collect())
          compareResult(hiveResStr, df2.collect())
          hiveClient.execute(table.majorCompaction)
          compareResult(hiveResStr, df1.collect())
          compareResult(hiveResStr, df2.collect())
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

            hiveClient.execute(table1.insertIntoHiveTableKeyRange(1, 15))
            hiveClient.execute(table2.insertIntoHiveTableKeyRange(10, 25))

            var hiveResStr = hiveClient.executeQuery(Table.hiveJoin(table1, table2))
            val sparkRes1 = sparkCollect(Table.sparkJoin(table1, table2))
            compareResult(hiveResStr, sparkRes1)
          }

          myRun(testName, code)
        }
      }
    }
  }

  /*
   * Utility functions
   */

  // Compare logic
  //
  // 1. Read entire table using hive client
  // 2. Read entire table using sparkSQL
  // 3. Read entire table using spark dataframe API
  // Verify: Both spark reads are same as hive read

  // Simple
  private def compare(table: Table): Unit = {
    log.info("++++ Verify simple")
    val hiveResStr = hiveClient.executeQuery(table.hiveSelect)
    val (dfFromSql, dfFromScala) = sparkGetDF(table)
    compareResult(hiveResStr, dfFromSql.collect())
    compareResult(hiveResStr, dfFromScala.collect())
  }
  // With Predicate
  private def compareWithPred(table: Table): Unit = {
    log.info("++++ Verify with predicate")
    val hiveResStr = hiveClient.executeQuery(table.hiveSelectWithPred)
    val (dfFromSql, dfFromScala) = sparkGetDFWithPred(table)
    compareResult(hiveResStr, dfFromSql.collect())
    compareResult(hiveResStr, dfFromScala.collect())
  }
  // With Projection
  private def compareWithProj(table: Table): Unit = {
    log.info("++++ Verify with projection")
    val hiveResStr = hiveClient.executeQuery(table.hiveSelectWithProj)
    val (dfFromSql, dfFromScala) = sparkGetDFWithProj(table)
    compareResult(hiveResStr, dfFromSql.collect())
    compareResult(hiveResStr, dfFromScala.collect())
  }

  // 1. Insert some more rows into the table using hive client.
  // 2. Compare simple
  // 3. Delete some rows from the table using hive client.
  // 4. Compare simple
  // 5. Update some rows in the table using hive client.
  // 4. Compare simple
  private def verify(table: Table, insertOnly: Boolean): Unit = {
    log.info("++ Verify")

    // Check results from Spark
    log.info("+++ Basic Check")
    compare(table)

    // Insert more rows in the table from Hive and compare result from Hive and Spark
    log.info("+++ After Insert")
    hiveClient.execute(table.insertIntoHiveTableKeyRange(10, 20))
    compare(table)

    // Short cut for insert only
    if (insertOnly) {
      return
    }

    // Update some rows in the table from Hive and compare result from Hive and Spark
    log.info("+++ After Update")
    hiveClient.execute(table.updateInHiveTableKey(13))
    hiveClient.execute(table.updateInHiveTableKey(19))
    compare(table)

    // delete some rows in the table from Hive and compare result from Hive and Spark
    log.info("+++ After Delete")
    hiveClient.execute(table.deleteFromHiveTableKey(12))
    hiveClient.execute(table.deleteFromHiveTableKey(18))
    compare(table)
  }

  // 1. Insert some more rows into the table using hive client.
  // 2. Compare simple/with predicate/with projection
  // 3. Delete some rows from the table using hive client.
  // 4. Compare simple/with predicate/with projection
  // 5. Update some rows in the table using hive client.
  // 6. Compare simple/with predicate/with projection
  private def verifyAll(table: Table, insertOnly: Boolean): Unit = {
    log.info("++ Verify All")
    // Check results from Spark

    log.info("+++ Basic Check")
    compare(table)
    compareWithPred(table)
    compareWithProj(table)


    // Insert more rows in the table from Hive and compare result from Hive and Spark
    log.info("+++ After Insert")
    hiveClient.execute(table.insertIntoHiveTableKeyRange(10, 20))
    compare(table)
    compareWithPred(table)
    compareWithProj(table)

    // Short cut for insert only
    if (insertOnly) {
      return
    }

    // Update some rows in the table from Hive and compare result from Hive and Spark
    log.info("+++ After Update")
    hiveClient.execute(table.updateInHiveTableKey(13))
    hiveClient.execute(table.updateInHiveTableKey(19))
    compare(table)
    compareWithPred(table)
    compareWithProj(table)

    // delete some rows in the table from Hive and compare result from Hive and Spark
    log.info("+++ After Delete")
    hiveClient.execute(table.deleteFromHiveTableKey(12))
    hiveClient.execute(table.deleteFromHiveTableKey(18))
    compare(table)
    compareWithPred(table)
    compareWithProj(table)
  }

  private def sparkGetDFWithProj(table: Table): (DataFrame, DataFrame) = {
    val dfSql = sparkSQL(table.sparkSelect)

    var dfScala = spark.read.format("HiveAcid").options(Map("table" -> table.hiveTname)).load().select(table.sparkDFProj)
    dfScala = totalOrderBy(table, dfScala)
    return (dfSql, dfScala)
  }

  private def sparkGetDFWithPred(table: Table): (DataFrame, DataFrame) = {
    val dfSql = sparkSQL(table.sparkSelect)

    var dfScala = spark.read.format("HiveAcid").options(Map("table" -> table.hiveTname)).load().where(col("intCol") < "5")
    dfScala = totalOrderBy(table, dfScala)
    return (dfSql, dfScala)
  }

  private def sparkGetDF(table: Table): (DataFrame, DataFrame) = {
    val dfSql = sparkSQL(table.sparkSelect)

    var dfScala = spark.read.format("HiveAcid").options(Map("table" -> table.hiveTname)).load()
    dfScala = totalOrderBy(table, dfScala)
    return (dfSql, dfScala)
  }

  private def sparkSQL(cmd: String): DataFrame = {
    log.debug(s"SparkSQL> ${cmd}")
    spark.sql(cmd)
  }

  private def sparkCollect(cmd: String): Array[Row] = {
    log.debug(s"SparkSQL> ${cmd}")
    spark.sql(cmd).collect()
  }

  private def totalOrderBy(table: Table, df: DataFrame): DataFrame = {
    val colSeq = table.getColMap.map{case(colName, _) => col(colName)}.toSeq
    df.orderBy(colSeq:_*)
  }

  private def recreate(table: Table, createSymlinkSparkTables: Boolean = true): Unit = {
    log.info("++ Recreate Table")
    sparkCollect(table.sparkDrop)
    hiveClient.execute(table.hiveDrop)
    hiveClient.execute(table.hiveCreate)
    if (createSymlinkSparkTables) {
      sparkCollect(table.sparkCreate)
    }
  }

  // Compare the results
  private def compareResult(hiveResStr: String, sparkRes: Array[Row]): Unit = {
    val sparkResStr = sparkRowsToStr(sparkRes)
    log.debug(s"Comparing \n hive: $hiveResStr \n Spark: $sparkResStr")
    assert(hiveResStr == sparkResStr)
  }

  // Convert Array of Spark Rows into a String
  private def sparkRowsToStr(rows: Array[Row]): String = {
    rows.map(row => row.mkString(",")).mkString("\n")
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
