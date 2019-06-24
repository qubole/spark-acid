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
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util._

// scalastyle:off: removeFile
class HiveACIDSuite extends QueryTest
  with SharedSQLContext {


  val hClient = new TestHiveClient()

  testQuietly("checkpoint") {

    //println("----------------------> DUMMY TEST      ---------------------> ")
    //hClient.init()

    //println("----------------------> DUMMY TEST      ---------------------> ")
    //hClient.execute("show databases;")

    //println("----------------------> DUMMY TEST      ---------------------> ")
    //hClient.execute("show tables;")

    println("----------------------> DUMMY TEST      ---------------------> ")
    spark.sql("show tables").collect.foreach(println)

    println("----------------------> DUMMY TEST      ---------------------> ")
    spark.sql("show databases").collect.foreach(println)

    //println("----------------------> DUMMY TEST      ---------------------> ")
    //hClient.teardown()

  }
}
