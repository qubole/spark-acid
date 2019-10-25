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

import java.io.StringWriter
import java.net.URLClassLoader
import java.net.URL

import org.apache.commons.logging.LogFactory
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.util._

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

import scala.util.control.NonFatal

class TestHelper {

  import TestHelper._
  var spark : SparkSession = _
  var hiveClient : TestHiveClient = _

  var verbose = false

  def init(isDebug: Boolean) {
    verbose = isDebug
    // Clients
    spark = TestSparkSession.getSession()
    if (verbose) {
      log.setLevel(Level.DEBUG)
    }
    hiveClient = new TestHiveClient()
  }

  def destroy(): Unit = {
    hiveClient.teardown()
    spark.stop()
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

  // Check the data present in this table via hive as well as spark sql and df
  private def compare(table: Table, msg: String): Unit = {
    log.info(s"Verify simple ${msg}")
    val hiveResStr = hiveExecuteQuery(table.hiveSelect)
    val (dfFromSql, dfFromScala) = sparkGetDF(table)
    compareResult(hiveResStr, dfFromSql.collect())
    compareResult(hiveResStr, dfFromScala.collect())
  }

  // With Predicate
  private def compareWithPred(table: Table, msg: String, pred: String): Unit = {
    log.info(s"Verify with predicate ${msg}")
    val hiveResStr = hiveExecuteQuery(table.hiveSelectWithPred(pred))
    val (dfFromSql, dfFromScala) = sparkGetDFWithPred(table, pred)
    compareResult(hiveResStr, dfFromSql.collect())
    compareResult(hiveResStr, dfFromScala.collect())
  }
  // With Projection
  private def compareWithProj(table: Table, msg: String): Unit = {
    log.info(s"Verify with projection ${msg}")
    val hiveResStr = hiveExecuteQuery(table.hiveSelectWithProj)
    val (dfFromSql, dfFromScala) = sparkGetDFWithProj(table)
    compareResult(hiveResStr, dfFromSql.collect())
    compareResult(hiveResStr, dfFromScala.collect())
  }

  // Compare result of 2 tables via hive
  def compareTwoTablesViaHive(table1: Table, table2: Table, msg: String,
                                      expectedRows: Int = -1): Unit = {
    log.info(s"Verify output of 2 tables via Hive: ${msg}")
    val hiveResStr1 = hiveExecuteQuery(table1.hiveSelect)
    val hiveResStr2 = hiveExecuteQuery(table2.hiveSelect)
    assert(hiveResStr1 == hiveResStr2, s"out1: \n${hiveResStr1}\nout2: \n${hiveResStr2}\n")
    if (expectedRows != -1) {
      val resultRows = hiveResStr1.split("\n").length
      assert(resultRows == expectedRows, s"Expected $expectedRows rows, got $resultRows rows " +
        s"in output:\n$hiveResStr1")
    }
  }

  // Compare result of 2 tables via spark
  def compareTwoTablesViaSpark(table1: Table, table2: Table, msg: String,
                               expectedRows: Int = -1): Unit = {
    log.info(s"Verify output of 2 tables via Spark: ${msg}")
    val sparkResRows1 = sparkCollect(table1.hiveSelect)
    val sparkResRows2 = sparkCollect(table2.hiveSelect)
    compareResult(sparkResRows1, sparkResRows2)
    if (expectedRows != -1) {
      val result = sparkRowsToStr(sparkResRows1)
      val resultRows = result.split("\n").length
      assert(resultRows == expectedRows, s"Expected $expectedRows rows, got $resultRows rows " +
        s"in output:\n$result")
    }
  }

  // 1. Insert some more rows into the table using hive client.
  // 2. Compare simple
  // 3. Delete some rows from the table using hive client.
  // 4. Compare simple
  // 5. Update some rows in the table using hive client.
  // 4. Compare simple
  def verify(table: Table, insertOnly: Boolean): Unit = {
    // Check results from Spark
    compare(table, "")

    // Insert more rows in the table from Hive and compare result from Hive and Spark
    hiveExecute(table.insertIntoHiveTableKeyRange(10, 20))
    compare(table, "After Insert")

    // Short cut for insert only
    if (insertOnly) {
      return
    }

    // Update some rows in the table from Hive and compare result from Hive and Spark
    hiveExecute(table.updateInHiveTableKey(13))
    hiveExecute(table.updateInHiveTableKey(19))
    compare(table, "After Update")

    // delete some rows in the table from Hive and compare result from Hive and Spark
    hiveExecute(table.deleteFromHiveTableKey(12))
    hiveExecute(table.deleteFromHiveTableKey(18))
    compare(table, "After Delete")
  }

  // 1. Insert some more rows into the table using hive client.
  // 2. Compare simple/with predicate/with projection
  // 3. Delete some rows from the table using hive client.
  // 4. Compare simple/with predicate/with projection
  // 5. Update some rows in the table using hive client.
  // 6. Compare simple/with predicate/with projection
  def verifyWithPred(table: Table, insertOnly: Boolean, pred: String): Unit = {
    // Check results from Spark

    compare(table, "")
    compareWithPred(table, "", pred)
    compareWithProj(table, "")


    // Insert more rows in the table from Hive and compare result from Hive and Spark
    log.info("+++ After Insert\n")
    hiveExecute(table.insertIntoHiveTableKeyRange(10, 20))
    compare(table, "After Insert")
    compareWithPred(table, "After Insert", pred)
    compareWithProj(table, "After Insert")

    // Short cut for insert only
    if (insertOnly) {
      return
    }

    // Update some rows in the table from Hive and compare result from Hive and Spark
    hiveExecute(table.updateInHiveTableKey(13))
    hiveExecute(table.updateInHiveTableKey(19))
    compare(table, "After Update")
    compareWithPred(table, "After Update", pred)
    compareWithProj(table, "After Update")

    // delete some rows in the table from Hive and compare result from Hive and Spark
    hiveExecute(table.deleteFromHiveTableKey(12))
    hiveExecute(table.deleteFromHiveTableKey(18))
    compare(table, "After Delete")
    compareWithPred(table, "After Delete", pred)
    compareWithProj(table, "After Delete")
  }

  def sparkGetDFWithProj(table: Table): (DataFrame, DataFrame) = {
    val dfSql = sparkSQL(table.sparkSelect)

    var dfScala = spark.read.format("HiveAcid").options(Map("table" -> table.hiveTname)).load().select(table.sparkDFProj)
    dfScala = totalOrderBy(table, dfScala)
    return (dfSql, dfScala)
  }

  def sparkGetDFWithPred(table: Table, pred: String): (DataFrame, DataFrame) = {
    val dfSql = sparkSQL(table.sparkSelectWithPred(pred))

    var dfScala = spark.read.format("HiveAcid").options(Map("table" -> table.hiveTname)).load().where(col("intCol") < "5")
    dfScala = totalOrderBy(table, dfScala)
    return (dfSql, dfScala)
  }

  def sparkGetDF(table: Table): (DataFrame, DataFrame) = {
    val dfSql = sparkSQL(table.sparkSelect)

    var dfScala = spark.read.format("HiveAcid").options(Map("table" -> table.hiveTname)).load()
    dfScala = totalOrderBy(table, dfScala)
    return (dfSql, dfScala)
  }

  def sparkSQL(cmd: String): DataFrame = {
    log.debug(s"Spark> ${cmd}\n")
    spark.sql(cmd)
  }

  def sparkCollect(cmd: String): Array[Row] = {
    log.debug(s"Spark> ${cmd}\n")
    spark.sql(cmd).collect()
  }

  def hiveExecute(cmd: String): Any = {
	log.debug(s"Hive>  ${cmd}\n");
    hiveClient.execute(cmd)
  }

  def hiveExecuteQuery(cmd: String): String = {
	log.debug(s"Hive>  ${cmd}\n");
    hiveClient.executeQuery(cmd)
  }

  private def totalOrderBy(table: Table, df: DataFrame): DataFrame = {
    val colSeq = table.getColMap.map{case(colName, _) => col(colName)}.toSeq
    df.orderBy(colSeq:_*)
  }

  def recreate(table: Table, createSymlinkSparkTables: Boolean = true): Unit = {
    log.info("++ Recreate Table")
    sparkCollect(table.sparkDrop)
    hiveExecute(table.hiveDrop)
    hiveExecute(table.hiveCreate)
    if (createSymlinkSparkTables) {
      sparkCollect(table.sparkCreate)
    }
    hiveExecute(table.disableCompaction)
  }

  // Compare the results
  def compareResult(hiveResStr: String, sparkRes: Array[Row]): Unit = {
    val sparkResStr = sparkRowsToStr(sparkRes)
    log.debug(s"Comparing \n hive: $hiveResStr \n Spark: $sparkResStr")
    assert(hiveResStr == sparkResStr)
  }

  // Compare the results
  def compareResult(sparkRes1: Array[Row], sparkRes2: Array[Row]): Unit = {
    val sparkResStr1 = sparkRowsToStr(sparkRes1)
    val sparkResStr2 = sparkRowsToStr(sparkRes2)
    log.debug(s"Comparing \n hive: $sparkResStr1 \n Spark: $sparkResStr2")
    assert(sparkResStr1 == sparkResStr2)
  }

  // Convert Array of Spark Rows into a String
  private def sparkRowsToStr(rows: Array[Row]): String = {
    rows.map(row => row.mkString(",")).mkString("\n")
  }


  // Run single type run
  @throws(classOf[Exception])
  def myRun(testName: String, code: () => Unit): Unit = {
    try {
      log.info(s">>>>>>>>>>>>>>>>>>> $testName")
      code()
    } catch {
      case NonFatal(e) =>
        log.info(s"Failed test[${testName}]:$e")
        throw e
    }
  }

  def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val conf = spark.sessionState.conf
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (conf.contains(key)) {
        Some(conf.getConfString(key))
      } else {
        None
      }
    }
    (keys, values).zipped.foreach { (k, v) =>
      if (SQLConf.staticConfKeys.contains(k)) {
        throw new AnalysisException(s"Cannot modify the value of a static config: $k")
      }
      conf.setConfString(k, v)
    }
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
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

object TestHelper {

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
