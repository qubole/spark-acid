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

import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}

import com.qubole.shaded.hadoop.hive.common.{ValidTxnWriteIdList, ValidWriteIdList}
import com.qubole.shaded.hadoop.hive.conf.HiveConf
import com.qubole.shaded.hadoop.hive.metastore.api.{DataOperationType, LockRequest, LockResponse, LockState}
import com.qubole.shaded.hadoop.hive.metastore.conf.MetastoreConf
import com.qubole.shaded.hadoop.hive.metastore.txn.TxnUtils
import com.qubole.shaded.hadoop.hive.metastore.{HiveMetaStoreClient, LockComponentBuilder, LockRequestBuilder}
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.thrift.TException

import scala.collection.JavaConversions._
import scala.language.implicitConversions

class HiveAcidTxnManager(sparkSession: SparkSession,
                         val hiveConf: HiveConf,
                         val table: HiveAcidTable,
                         val writerOperationType: HiveAcidOperation.OperationType
                        ) extends Logging {

  private val heartbeatInterval = MetastoreConf.getTimeVar(hiveConf,
    MetastoreConf.ConfVars.TXN_TIMEOUT, TimeUnit.MILLISECONDS) / 2

  private val user: String = sparkSession.sparkContext.sparkUser
  private var _client: HiveMetaStoreClient = _
  private var txnId: Long = -1
  private var currentWriteIdForTable: Long = -1
  private var isTxnClosed = false
  private var heartBeater: ScheduledExecutorService = _
  private lazy val heartBeaterClient: HiveMetaStoreClient =
    new HiveMetaStoreClient(hiveConf, null, false)
  private var nextSleep: Long = _
  private var MAX_SLEEP: Long = _

  val location: Path = table.rootPath

  def begin(partitionNames: Seq[String]): Unit = {
    if (txnId != -1) {
      throw HiveAcidErrors.txnAlreadyOpen(txnId)
    }
    // 1. Open transaction
    txnId = client.openTxn(HiveAcidUtils.NAME) // TODO change this to user instead
    logInfo("Opened txnid: " + txnId + " for table " + table.fullyQualifiedName)
    isTxnClosed = false
    // 2. Start HeartBeater
    if (heartBeater == null) {
      heartBeater = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        def newThread(r: Runnable) = new HeartBeaterThread(r, "AcidDataSourceHeartBeater")
      })
    } else {
      throw HiveAcidErrors.heartBeaterAlreadyExists
    }
    heartBeater.scheduleAtFixedRate(new HeartbeatRunnable(),
      (heartbeatInterval * 0.75 * Math.random()).asInstanceOf[Long],
      heartbeatInterval,
      TimeUnit.MILLISECONDS)

    // 3. Acquire locks
    acquireLocks(partitionNames)
  }

  def end(abort: Boolean = false): Unit = {
    if (txnId != -1 && !isTxnClosed) {
      synchronized {
        if (txnId != -1 && !isTxnClosed) {
          try {
            logInfo(s"Closing txnid: $txnId for table ${table.fullyQualifiedName}. abort = $abort")
            if (abort) {
              client.abortTxns(Seq(txnId).asInstanceOf[java.util.List[java.lang.Long]])
            } else {
              client.commitTxn(txnId)
            }
            closeClient()
            txnId = -1
            isTxnClosed = true
            heartBeater.shutdown() // TODO: is this ok to call from here?
            heartBeater = null
          } finally {
            heartBeaterClient.close()
          }
        } else {
          logWarning("Transaction already closed")
        }
      }
    } else {
      logWarning("Transaction already closed")
    }
  }

  private def client: HiveMetaStoreClient = {
    if (_client == null) {
      _client = new HiveMetaStoreClient(hiveConf, null, false)
    }
    _client

  }

  private def closeClient(): Unit = {
    if (_client != null) {
      _client.close()
      _client = null
    }
  }

  private def acquireLocks(partitionNames: Seq[String]): Unit = {
    if (isTxnClosed || (txnId == -1)) {
      logError("Transaction already closed")
      throw HiveAcidErrors.txnClosedException
    }
    val req: LockRequest = createLockRequest(partitionNames)
    lock(req)
  }

  lazy val getValidWriteIds: ValidWriteIdList = {
    val validTxns = client.getValidTxns(txnId)
    val txnWriteIds: ValidTxnWriteIdList = TxnUtils.createValidTxnWriteIdList(txnId,
      client.getValidWriteIds(Seq(table.fullyQualifiedName),
        validTxns.writeToString()))
    txnWriteIds.getTableValidWriteIdList(table.fullyQualifiedName)
  }

  /* Unused for now.
   * Use this instead of open(), acquireLocks(), getValidWriteIds() and close() if not doing transaction management.
   */
  lazy val getValidWriteIdsNoTxn: ValidWriteIdList = {
    val validTxns = client.getValidTxns()
    val txnWriteIds: ValidTxnWriteIdList = TxnUtils.createValidTxnWriteIdList(txnId,
      client.getValidWriteIds(Seq(table.fullyQualifiedName),
        validTxns.writeToString()))
    txnWriteIds.getTableValidWriteIdList(table.fullyQualifiedName)
  }

  private class HeartbeatRunnable() extends Runnable {
    override def run(): Unit = {
      try {
        if (txnId > 0 && !isTxnClosed) {
          val resp = heartBeaterClient.heartbeatTxnRange(txnId, txnId)
          if (!resp.getAborted.isEmpty || !resp.getNosuch.isEmpty) {
            logError("Heartbeat failure: " + resp.toString)
            try {
              isTxnClosed = true
              // TODO: IS SHUTDOWN THE RIGHT THING TO DO HERE?
              heartBeater.shutdown()
              heartBeater = null
            } finally {
              heartBeaterClient.close()
            }
          } else {
            logInfo("Heartbeat sent for txnId: " + txnId)
          }
        }
      }
      catch {
        case e: TException =>
          logWarning("Failure to heartbeat for txnId: " + txnId, e)
      }
    }
  }

  class HeartBeaterThread(val target: Runnable, val name: String) extends Thread(target, name) {
    setDaemon(true)
  }

  private def createLockRequest(partNames: Seq[String]) = {
    val requestBuilder = new LockRequestBuilder(HiveAcidUtils.NAME)
    requestBuilder.setUser(user)
    requestBuilder.setTransactionId(txnId)
    def addLockTypeToLockComponentBuilder(
      lcb: LockComponentBuilder): LockComponentBuilder = {
        writerOperationType match {
          case HiveAcidOperation.INSERT_OVERWRITE =>
            lcb.setExclusive().setOperationType(DataOperationType.UPDATE)
          case HiveAcidOperation.INSERT_INTO =>
            lcb.setShared().setOperationType(DataOperationType.INSERT)
          case HiveAcidOperation.UPDATE =>
            lcb.setSemiShared().setOperationType(DataOperationType.UPDATE)
          case HiveAcidOperation.DELETE =>
            lcb.setSemiShared().setOperationType(DataOperationType.DELETE)
          case HiveAcidOperation.READ =>
            lcb.setShared().setOperationType(DataOperationType.SELECT)
          case _ =>
            throw HiveAcidErrors.invalidOperationType(writerOperationType.toString)
        }
    }
    if (partNames.isEmpty) {
      val lockCompBuilder = new LockComponentBuilder()
        .setDbName(table.dbName)
        .setTableName(table.tableName)

      requestBuilder.addLockComponent(addLockTypeToLockComponentBuilder(lockCompBuilder).build)
    } else {
      partNames.foreach(partName => {
        val lockCompBuilder = new LockComponentBuilder()
          .setPartitionName(partName)
          .setDbName(table.dbName)
          .setTableName(table.tableName)
        requestBuilder.addLockComponent(addLockTypeToLockComponentBuilder(lockCompBuilder).build)
      })
    }
    requestBuilder.build
  }

  private def lock(lockReq: LockRequest): Unit = {
    nextSleep = 50
    /* MAX_SLEEP is the max time each backoff() will wait for, thus the total time to wait for
    successful lock acquisition is approximately (see backoff()) maxNumWaits * MAX_SLEEP.
     */
    val defaultMaxSleep = hiveConf.getTimeVar(
      HiveConf.ConfVars.HIVE_LOCK_SLEEP_BETWEEN_RETRIES, TimeUnit.MILLISECONDS)
    MAX_SLEEP = Math.max(15000, defaultMaxSleep)
    val maxNumWaits: Int = Math.max(0, hiveConf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_NUMRETRIES))
    try {
      var res: LockResponse = client.lock(lockReq)
      // link lockId to queryId
      var numRetries: Int = 0
      while (res.getState == LockState.WAITING && numRetries < maxNumWaits) {
        numRetries += 1
        backoff()
        res = client.checkLock(res.getLockid)
      }
      if (res.getState != LockState.ACQUIRED) {
        throw HiveAcidErrors.couldNotAcquireLockException(state = res.getState.name())
      }
    } catch {
      case e: TException =>
        logWarning("Unable to acquire lock", e)
        throw HiveAcidErrors.couldNotAcquireLockException(e)
    }
  }

  private def backoff(): Unit = {
    nextSleep *= 2
    if (nextSleep > MAX_SLEEP) nextSleep = MAX_SLEEP
    try
      Thread.sleep(nextSleep)
    catch {
      case e: InterruptedException =>

    }
  }

  def getTxnId: Long = {
    txnId
  }

  def getCurrentWriteIdForTable(): Long = {
    if (txnId == -1) {
      throw HiveAcidErrors.tableWriteIdRequestedBeforeTxnStart(table.fullyQualifiedName)
    }
    if (currentWriteIdForTable != -1) {
      return currentWriteIdForTable
    }
    currentWriteIdForTable = client.allocateTableWriteId(txnId, table.dbName, table.tableName)
    return currentWriteIdForTable
  }
}
