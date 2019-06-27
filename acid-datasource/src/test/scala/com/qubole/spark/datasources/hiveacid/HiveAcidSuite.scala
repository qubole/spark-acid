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

import org.apache.spark.sql._
import org.scalatest._
import org.apache.spark.util._
import scala.util.control.NonFatal

class HiveACIDSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  var spark : SparkSession = _
  var hiveClient : TestHiveClient = _

  val DBNAME: String = "HiveTestDB"

  override def beforeAll() {

    // Create clients
    spark = TestSparkSession.getSession()
    hiveClient = new TestHiveClient()

    // Create fresh new database
    /*
    try {
      hiveClient.execute("Drop database "+ DBNAME)
    } catch {
      case e Throwable: println("Failed to drop database ..." + e) 
    }
    hiveClient.execute("Create database "+ DBNAME)
    */
  }

  /*
  @throws(classOf[Exception])
  def createTable(tblname: String): Boolean = {
    if (! hiveClient.execute("create table "+"DBNAME"+"."+tblname+ " (key int, value string)") {
      return false;
    }
    return true
  }

  @throws(classOf[Exception])
  def dropTable(tblname: String): Boolean = {
    if (! hiveClient.execute("drop table "+"DBNAME"+"."+tblname")) {
      return false;
    }
    return true
  }
  */

  test("your test name here"){
    try {

      println("HIVE ------------------------------------------------------------------------------")
      val hiveQuery = "select * from default.acidtbl"
      val res = hiveClient.executeQuery(hiveQuery)
      println("Query: " + hiveQuery)
      println("Result: " + hiveClient.resultStr(res))
      res.close()
  
      println("DataFrame ---------------------------------------------------------------------------")
      spark.read.format("HiveAcid").options(Map("table" -> "default.acidtbl")).load().collect().foreach(println)
      
      println("SQL ------------------------------------------------------------------------------")
      val sparkQuery = "select * from default.sparkacidtbl"
      println("Query: " + sparkQuery)
      spark.sql(sparkQuery).collect.foreach(println)
    }
    catch {
      case NonFatal(e) => println("Got some other kind of exception " + e)
    }
  }

  override protected def afterAll(): Unit = {
    hiveClient.teardown()
    spark.stop()
  }
}
