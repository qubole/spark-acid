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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets.UTF_8

import org.apache.spark.sql.execution.streaming.CompactibleFileStreamLog


class HiveAcidSinkLogSuite extends HiveAcidStreamingFunSuite  {

  import CompactibleFileStreamLog._
  import HiveAcidSinkLog._
  import HiveAcidSinkOptions._


  test("compactLogs") {
    withHiveAcidSinkLog { sinkLog =>
      val logs = Seq(
        newFakeHiveAcidSinkStatus(0, HiveAcidSinkLog.ADD_ACTION),
        newFakeHiveAcidSinkStatus(1, HiveAcidSinkLog.ADD_ACTION),
        newFakeHiveAcidSinkStatus(2, HiveAcidSinkLog.ADD_ACTION))
      assert(logs === sinkLog.compactLogs(logs))

      val logs2 = Seq(
        newFakeHiveAcidSinkStatus(3, HiveAcidSinkLog.ADD_ACTION),
        newFakeHiveAcidSinkStatus(4, HiveAcidSinkLog.ADD_ACTION),
        newFakeHiveAcidSinkStatus(2, HiveAcidSinkLog.DELETE_ACTION))
      assert(logs.dropRight(1) ++ logs2.dropRight(1) === sinkLog.compactLogs(logs ++ logs2))
    }
  }

  test("serialize") {
    withHiveAcidSinkLog { sinkLog =>
      val logs = Array(
        HiveAcidSinkStatus(0, HiveAcidSinkLog.ADD_ACTION),
        HiveAcidSinkStatus(1, HiveAcidSinkLog.DELETE_ACTION),
        HiveAcidSinkStatus(2, HiveAcidSinkLog.ADD_ACTION))

      // scalastyle:off
      val expected = s"""v$VERSION
                        |{"txnId":0,"action":"add"}
                        |{"txnId":1,"action":"delete"}
                        |{"txnId":2,"action":"add"}""".stripMargin
      // scalastyle:on
      val baos = new ByteArrayOutputStream()
      sinkLog.serialize(logs, baos)
      assert(expected === baos.toString(UTF_8.name()))
      baos.reset()
      sinkLog.serialize(Array(), baos)
      assert(s"v$VERSION" === baos.toString(UTF_8.name()))
    }
  }

  test("deserialize") {
    withHiveAcidSinkLog { sinkLog =>
      // scalastyle:off
      val logs = s"""v$VERSION
                    |{"txnId":0,"action":"add"}
                    |{"txnId":1,"action":"delete"}
                    |{"txnId":2,"action":"add"}""".stripMargin
      // scalastyle:on

      val expected = Seq(
        HiveAcidSinkStatus(0, HiveAcidSinkLog.ADD_ACTION),
        HiveAcidSinkStatus(1, HiveAcidSinkLog.DELETE_ACTION),
        HiveAcidSinkStatus(2, HiveAcidSinkLog.ADD_ACTION))

      assert(expected === sinkLog.deserialize(new ByteArrayInputStream(logs.getBytes(UTF_8))))

      assert(Nil === sinkLog.deserialize(new ByteArrayInputStream(s"v$VERSION".getBytes(UTF_8))))
    }
  }

  test("compact") {
    val options = Map(COMPACT_INTERVAL_KEY -> "3")
      withHiveAcidSinkLog( { sinkLog =>
        for (batchId <- 0 to 10) {
          sinkLog.add(
            batchId,
            Array(newFakeHiveAcidSinkStatus(batchId, HiveAcidSinkLog.ADD_ACTION)))
          val expectedFiles = (0 to batchId).map {
            id => newFakeHiveAcidSinkStatus(id, HiveAcidSinkLog.ADD_ACTION)
          }
          assert(sinkLog.allFiles() === expectedFiles)
          if (isCompactionBatch(batchId, 3)) {
            // Since batchId is a compaction batch, the batch log file should contain all logs
            assert(sinkLog.get(batchId).getOrElse(Nil) === expectedFiles)
          }
        }
      }, options)
  }

  test("delete expired file") {
    // Set Acid Sink Log Cleanup delay to 0 so that we can detect the deleting behaviour
    // deterministically and one min batches to retain
    val options1 = Map(
      COMPACT_INTERVAL_KEY -> "3",
      CLEANUP_DELAY_KEY -> "0",
      MIN_BATCHES_TO_RETAIN_KEY -> "1"
    )
      withHiveAcidSinkLog( { sinkLog =>
        val fs = sinkLog.metadataPath.getFileSystem(helper.spark.sessionState.newHadoopConf())

        def listBatchFiles(): Set[String] = {
          fs.listStatus(sinkLog.metadataPath).map(_.getPath.getName).filter { fileName =>
            try {
              getBatchIdFromFileName(fileName)
              true
            } catch {
              case _: NumberFormatException => false
            }
          }.toSet
        }

        sinkLog.add(0, Array(newFakeHiveAcidSinkStatus(0, HiveAcidSinkLog.ADD_ACTION)))
        assert(Set("0") === listBatchFiles())
        sinkLog.add(1, Array(newFakeHiveAcidSinkStatus(1, HiveAcidSinkLog.ADD_ACTION)))
        assert(Set("0", "1") === listBatchFiles())
        sinkLog.add(2, Array(newFakeHiveAcidSinkStatus(2, HiveAcidSinkLog.ADD_ACTION)))
        assert(Set("0", "1", "2.compact") === listBatchFiles())
        sinkLog.add(3, Array(newFakeHiveAcidSinkStatus(3, HiveAcidSinkLog.ADD_ACTION)))
        assert(Set("2.compact", "3") === listBatchFiles())
        sinkLog.add(4, Array(newFakeHiveAcidSinkStatus(4, HiveAcidSinkLog.ADD_ACTION)))
        assert(Set("2.compact", "3", "4") === listBatchFiles())
        sinkLog.add(5, Array(newFakeHiveAcidSinkStatus(5, HiveAcidSinkLog.ADD_ACTION)))
        assert(Set("2.compact", "3", "4", "5.compact") === listBatchFiles())
        sinkLog.add(6, Array(newFakeHiveAcidSinkStatus(6, HiveAcidSinkLog.ADD_ACTION)))
        assert(Set("5.compact", "6") === listBatchFiles())
      }, options1)

    val options2 = Map(
      COMPACT_INTERVAL_KEY -> "3",
      CLEANUP_DELAY_KEY -> "0",
      MIN_BATCHES_TO_RETAIN_KEY -> "2"
    )

      withHiveAcidSinkLog( { sinkLog =>
        val fs = sinkLog.metadataPath.getFileSystem(helper.spark.sessionState.newHadoopConf())

        def listBatchFiles(): Set[String] = {
          fs.listStatus(sinkLog.metadataPath).map(_.getPath.getName).filter { fileName =>
            try {
              getBatchIdFromFileName(fileName)
              true
            } catch {
              case _: NumberFormatException => false
            }
          }.toSet
        }

        sinkLog.add(0, Array(newFakeHiveAcidSinkStatus(0, HiveAcidSinkLog.ADD_ACTION)))
        assert(Set("0") === listBatchFiles())
        sinkLog.add(1, Array(newFakeHiveAcidSinkStatus(1, HiveAcidSinkLog.ADD_ACTION)))
        assert(Set("0", "1") === listBatchFiles())
        sinkLog.add(2, Array(newFakeHiveAcidSinkStatus(2, HiveAcidSinkLog.ADD_ACTION)))
        assert(Set("0", "1", "2.compact") === listBatchFiles())
        sinkLog.add(3, Array(newFakeHiveAcidSinkStatus(3, HiveAcidSinkLog.ADD_ACTION)))
        assert(Set("0", "1", "2.compact", "3") === listBatchFiles())
        sinkLog.add(4, Array(newFakeHiveAcidSinkStatus(4, HiveAcidSinkLog.ADD_ACTION)))
        assert(Set("2.compact", "3", "4") === listBatchFiles())
        sinkLog.add(5, Array(newFakeHiveAcidSinkStatus(5, ADD_ACTION)))
        assert(Set("2.compact", "3", "4", "5.compact") === listBatchFiles())
        sinkLog.add(6, Array(newFakeHiveAcidSinkStatus(6, ADD_ACTION)))
        assert(Set("2.compact", "3", "4", "5.compact", "6") === listBatchFiles())
        sinkLog.add(7, Array(newFakeHiveAcidSinkStatus(7, ADD_ACTION)))
        assert(Set("5.compact", "6", "7") === listBatchFiles())
      }, options2)
  }

  /**
    * Create a fake HiveAcidSinkStatus using path and action.
    */
  private def newFakeHiveAcidSinkStatus(txnId: Long, action: String): HiveAcidSinkStatus = {
    HiveAcidSinkStatus(txnId, action)
  }

  private def withHiveAcidSinkLog(f: HiveAcidSinkLog => Unit,
                                  extraOptions: Map[String, String] = Map.empty): Unit = {
    val tempDir = helper.createCheckpointDir()
    val options = new HiveAcidSinkOptions(extraOptions ++ Map("table" -> "dummyTable"))
    val sinkLog = new HiveAcidSinkLog(HiveAcidSinkLog.VERSION, helper.spark, tempDir.getCanonicalPath, options)
    f(sinkLog)
    helper.deleteCheckpointDir(tempDir.getCanonicalPath)
  }

}