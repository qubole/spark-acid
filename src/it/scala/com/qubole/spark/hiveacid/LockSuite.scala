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

import com.qubole.spark.hiveacid.hive.HiveAcidMetadata
import com.qubole.spark.hiveacid.transaction.HiveAcidTxn
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import scala.util.control.NonFatal

class LockSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  val log: Logger = LogManager.getLogger(this.getClass)
  log.setLevel(Level.INFO)

  var helper: TestHelper = _
  val isDebug = true

  val DEFAULT_DBNAME =  "HiveTestLockDB"
  val cols: Map[String, String] = Map(
    ("intCol","int"),
    ("doubleCol","double"),
    ("floatCol","float"),
    ("booleanCol","boolean")
  )
  val partitionedTable = new Table(DEFAULT_DBNAME, "partitioned",
    cols, Table.orcPartitionedFullACIDTable, true)
  val normalTable = new Table(DEFAULT_DBNAME, "nonPartitioned",
    cols, Table.orcFullACIDTable, false)

  override def beforeAll() {
    try {
      helper = new TestLockHelper
      if (isDebug) {
        log.setLevel(Level.DEBUG)
      }
      helper.init(isDebug)

      // DB
      helper.hiveExecute("DROP DATABASE IF EXISTS "+ DEFAULT_DBNAME +" CASCADE")
      helper.hiveExecute("CREATE DATABASE "+ DEFAULT_DBNAME)
      helper.recreate(partitionedTable)
      helper.recreate(normalTable)
      helper.hiveExecute(partitionedTable.insertIntoHiveTableKeyRange(11, 25))
    } catch {
      case NonFatal(e) => log.info("failed " + e)
    }
  }

  override protected def afterAll(): Unit = {
    helper.hiveExecute(s"DROP TABLE IF EXISTS ${normalTable.hiveTname}")
    helper.hiveExecute(s"DROP TABLE IF EXISTS ${partitionedTable.hiveTname}")
    helper.hiveExecute("DROP DATABASE IF EXISTS "+ DEFAULT_DBNAME +" CASCADE")
    helper.destroy()
  }

  case class TestLockOperation(whichTransaction: Int,
                               operationType: HiveAcidOperation.OperationType,
                               partition: Seq[String],
                               willFail: Boolean = false)

  test("test lock wait timeout exception") {
    val lockOps = Seq(
      TestLockOperation(1, HiveAcidOperation.UPDATE, Seq()), // first trans will pass
      TestLockOperation(1, HiveAcidOperation.DELETE, Seq()), // similar operation on first trans will pass
      TestLockOperation(2, HiveAcidOperation.DELETE, Seq(), true)) // second transaction will wait and fail in 100ms
    testLockOps(lockOps)
  }

  test("test locks within same transaction is allowed") {
    val lockOps = Seq(
      TestLockOperation(1, HiveAcidOperation.UPDATE, Seq()), // first trans will pass
      TestLockOperation(1, HiveAcidOperation.DELETE, Seq()), // similar operation on first trans will pass
      TestLockOperation(1, HiveAcidOperation.READ, Seq()), // READ on same transaction will pass
      TestLockOperation(1, HiveAcidOperation.INSERT_OVERWRITE, Seq()))
    testLockOps(lockOps)
  }

  test("test READ after UPDATE/DELETE is allowed") {
    val lockOps = Seq(
      TestLockOperation(1, HiveAcidOperation.UPDATE, Seq()), // first trans will pass
      TestLockOperation(1, HiveAcidOperation.DELETE, Seq()),
      TestLockOperation(2, HiveAcidOperation.READ, Seq())) // second transaction READ need not wait
    testLockOps(lockOps)
  }

  test("test DELETE/READ after INSERT OVERWRITE is not allowed") {
    val lockOps = Seq(
      TestLockOperation(1, HiveAcidOperation.INSERT_OVERWRITE, Seq()),
      TestLockOperation(2, HiveAcidOperation.UPDATE, Seq(), true),
      TestLockOperation(2, HiveAcidOperation.DELETE, Seq(), true),
      TestLockOperation(2, HiveAcidOperation.READ, Seq(), true))
    testLockOps(lockOps)
  }

  test("test INSERT_OVERWRITE and DELETE/UPDATE/READ on different partition is allowed") {
    val lockOps = Seq(
      TestLockOperation(1, HiveAcidOperation.INSERT_OVERWRITE, Seq("ptnCol=0")),
      TestLockOperation(2, HiveAcidOperation.DELETE, Seq("ptnCol=1")),
      TestLockOperation(2, HiveAcidOperation.UPDATE, Seq("ptnCol=1")),
      TestLockOperation(2, HiveAcidOperation.READ, Seq("ptnCol=1")))
    testLockOps(lockOps)
  }

  def testLockOps(lockOps: Seq[TestLockOperation]): Unit = {
    val tableName = DEFAULT_DBNAME + "." + "nonPartitioned"
    val hiveAcidMetadata = HiveAcidMetadata.fromSparkSession(helper.spark,
      tableName)

    // Just try 2 attempts for lock acquisition and fail if it cannot.
    helper.spark.sessionState.conf.setConfString("spark.hiveAcid.lock.max.retries", "2")
    val sparkConf = SparkAcidConf(helper.spark, Map())
    val hTxn1 = new HiveAcidTxn(helper.spark)
    val hTxn2 = new HiveAcidTxn(helper.spark)

    def executeOp(lockOp: TestLockOperation) {
      val txn = lockOp.whichTransaction match {
        case 1 => hTxn1
        case 2 => hTxn2
        case _ => throw new IllegalArgumentException("Only 1 or 2 are supported for whichTransaction field")
      }
      if (lockOp.willFail) {
        val thrown = intercept[RuntimeException] {
          txn.acquireLocks(hiveAcidMetadata, lockOp.operationType, lockOp.partition, sparkConf)
        }
        assert(thrown.getMessage.contains("Could not acquire lock. Lock State: WAITING"))
       } else {
        txn.acquireLocks(hiveAcidMetadata, lockOp.operationType, lockOp.partition, sparkConf)
      }

    }

    try {
      hTxn1.begin()
      hTxn2.begin()
      lockOps.foreach(executeOp(_))
    } finally {
      helper.spark.sessionState.conf.unsetConf("spark.hiveAcid.lock.max.retries")
      hTxn1.end(true)
      hTxn2.end(true)
    }
  }

  test("test HeartBeatRunner is running") {
    val hTxn1 = new HiveAcidTxn(helper.spark)
    hTxn1.begin()
    // Sleep for 4 seconds
    Thread.sleep(4 * 1000)
    val txn = HiveAcidTxn.txnManager.showOpenTrans().find(ti => ti.getId == hTxn1.txnId)
    assert(txn.isDefined, "Transaction is expected to be open")
    val seconds = (txn.get.getLastHeartbeatTime() - txn.get.getStartedTime()) / 1000
    assert(seconds >= 2, "getLastHeartBeatTime should " +
      "be at least 2 seconds after transaction was opened")
    hTxn1.end(true)
  }
}

class TestLockHelper extends TestHelper {
  // Create spark session with txn timeout config as that needs to be set
  // before the start of spark session
  override def getSparkSession(): SparkSession = {
    SparkSession.builder().appName("Hive-acid-test")
      .master("local[*]")
      .config("spark.hadoop.hive.metastore.uris", "thrift://0.0.0.0:10000")
      .config("spark.sql.warehouse.dir", "/tmp")
      .config("spark.sql.extensions", "com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension")
      .config("spark.hadoop.hive.txn.timeout", "6")
      //.config("spark.ui.enabled", "true")
      //.config("spark.ui.port", "4041")
      // All V1 tests are executed USING HiveAcid
      .config("spark.hive.acid.datasource.version", "v2")
      .enableHiveSupport()
      .getOrCreate()
  }
}