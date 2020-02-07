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


package com.qubole.spark.hiveacid.streaming

import java.io.{File, IOException}
import java.util.UUID

import com.qubole.spark.hiveacid.TestHelper

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.scalatest.concurrent.TimeLimits
import org.scalatest.time.SpanSugar

class StreamingTestHelper extends TestHelper with TimeLimits  {

  import StreamingTestHelper._


  def runStreaming(tableName: String,
                   outputMode: OutputMode,
                   cols: Seq[String],
                   inputRange: Range,
                   options: List[(String, String)] = List.empty): Unit = {

    val inputData = MemoryStream[Int]
    val ds = inputData.toDS()

    val checkpointDir = createCheckpointDir(namePrefix = "stream.checkpoint").getCanonicalPath

    var query: StreamingQuery = null

    try {
      // Starting streaming query
      val writerDf =
        ds.map(i => (i*100, i*10, i))
          .toDF(cols:_*)
          .writeStream
          .format("HiveAcid")
          .option("table", tableName)
          .outputMode(outputMode)
          .option("checkpointLocation", checkpointDir)
          //.start()

      query = options.map { option =>
        writerDf.option(option._1, option._2)
      }.lastOption.getOrElse(writerDf).start()

      // Adding data for streaming query
      inputData.addData(inputRange)
      failAfter(STREAMING_TIMEOUT) {
        query.processAllAvailable()
      }
    } finally {
      if (query != null) {
        // Terminating streaming query
        query.stop()
        deleteCheckpointDir(checkpointDir)
      }
    }
  }

  def deleteCheckpointDir(fileStr: String): Unit = {
    val file = new File(fileStr)
    if (file != null) {
      JavaUtils.deleteRecursively(file)
    }
  }

  def createCheckpointDir(root: String = System.getProperty("java.io.tmpdir"),
                          namePrefix: String = "spark"): File = {

    var attempts = 0
    val maxAttempts = MAX_DIR_CREATION_ATTEMPTS
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: SecurityException => dir = null; }
    }
    dir.getCanonicalFile
  }

}

object StreamingTestHelper extends TestHelper with SpanSugar  {

  val MAX_DIR_CREATION_ATTEMPTS = 10
  val STREAMING_TIMEOUT = 60.seconds

}
