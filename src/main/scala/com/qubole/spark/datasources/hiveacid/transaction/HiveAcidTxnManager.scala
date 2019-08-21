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

package com.qubole.spark.datasources.hiveacid.transaction

import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}

import com.qubole.shaded.hadoop.hive.common.{ValidTxnWriteIdList, ValidWriteIdList}
import com.qubole.shaded.hadoop.hive.conf.HiveConf
import com.qubole.shaded.hadoop.hive.metastore.api.{DataOperationType, LockRequest, LockResponse, LockState}
import com.qubole.shaded.hadoop.hive.metastore.conf.MetastoreConf
import com.qubole.shaded.hadoop.hive.metastore.txn.TxnUtils
import com.qubole.shaded.hadoop.hive.metastore.{HiveMetaStoreClient, LockComponentBuilder, LockRequestBuilder}
import com.qubole.spark.datasources.hiveacid._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.thrift.TException

import scala.collection.JavaConversions._
import scala.language.implicitConversions

/**
 * Txn Manager for hive acid tables.
 * This takes care of creating txns, acquiring locks and sending heartbeats
 * @param sparkSession - Spark session
 * @param hiveConf - hive configuration to create hive metastore client
 */
private[hiveacid] class HiveAcidTxnManager(sparkSession: SparkSession,
                                           val hiveConf: HiveConf) extends Logging {

  private val heartbeatInterval = MetastoreConf.getTimeVar(hiveConf,
    MetastoreConf.ConfVars.TXN_TIMEOUT, TimeUnit.MILLISECONDS) / 2

  private lazy val client: HiveMetaStoreClient = new HiveMetaStoreClient(
    hiveConf, null, false)

  private lazy val heartBeaterClient: HiveMetaStoreClient =
    new HiveMetaStoreClient(hiveConf, null, false)
  // FIXME: Use threadpool so that we don't create multiple threads
  private var heartBeater: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      def newThread(r: Runnable) = new HeartBeaterThread(r, "AcidDataSourceHeartBeater")
    })
  heartBeater.scheduleAtFixedRate(
    new HeartbeatRunnable(),
    0,
    heartbeatInterval,
    TimeUnit.MILLISECONDS)

  private val user: String = sparkSession.sparkContext.sparkUser

  private val activeTxns = new scala.collection.mutable.HashMap[Long, HiveAcidFullTxn]()

  private var shutdownInitiated = false

  def beginTxn(txn: HiveAcidFullTxn): HiveAcidFullTxn = synchronized {
    // 1. Open transaction
    val txnId = client.openTxn(HiveAcidDataSource.NAME)
    if (activeTxns.contains(txnId)) {
      throw HiveAcidErrors.repeatedTxnId(txnId, activeTxns.keySet.toSeq)
    }
    txn.setTxnId(txnId)
    activeTxns.put(txnId, txn)
    logInfo("Opened txnid: " + txnId + " for table " + txn.acidTableMetadata.fullyQualifiedName)
    txn
  }

  def endTxn(txnId: Long, abort: Boolean = false): Unit = synchronized {
    try {
      if (abort) {
        client.abortTxns(Seq(txnId).asInstanceOf[java.util.List[java.lang.Long]])
      } else {
        client.commitTxn(txnId)
      }
    } catch {
      case e: Exception =>
        logWarning(s"Failure to end txn: ${txnId}, presumed abort", e)
    } finally {
      activeTxns.remove(txnId)
    }
  }

  def close(): Unit = synchronized {
    shutdownInitiated = true

    // Stop the heartbeat executor
    if (heartBeater != null) {
      heartBeater.shutdown()
    }

    // FIXME: 10 pulled out of thin air
    heartBeater.awaitTermination(10, TimeUnit.SECONDS)
    // abort all active transactions
    activeTxns.foreach {
      case (_, txn) => txn.end(true)
    }
    activeTxns.clear()

    // close all clients
    if (client != null) {
      client.close()
    }
    if (heartBeaterClient != null) {
      heartBeaterClient.close()
    }
  }

  def getCurrentWriteId(txnId: Long, acidTableMetadata: HiveAcidMetadata): Long = synchronized {
    client.allocateTableWriteId(txnId, acidTableMetadata.dbName, acidTableMetadata.tableName)
  }

  def getValidWriteIds(fullyQualifiedTableName: String): ValidWriteIdList = synchronized {
    getValidWriteIds(None, fullyQualifiedTableName)
  }

  def getValidWriteIds(txnId: Long,
                       fullyQualifiedTableName: String): ValidWriteIdList = synchronized {
    getValidWriteIds(Option(txnId), fullyQualifiedTableName)
  }

  private def getValidWriteIds(txnIdOpt: Option[Long],
                               fullyQualifiedTableName: String): ValidWriteIdList = synchronized {

    val (txnId, validTxns) = txnIdOpt match {
      case Some(id) => (id, client.getValidTxns(id))
      case None => (-1L, client.getValidTxns())
    }
    val tableValidWriteIds = client.getValidWriteIds(Seq(fullyQualifiedTableName),
      validTxns.writeToString())
    val txnWriteIds: ValidTxnWriteIdList = TxnUtils.createValidTxnWriteIdList(txnId,
      tableValidWriteIds)
    txnWriteIds.getTableValidWriteIdList(fullyQualifiedTableName)
  }

  def acquireLocks(txnId: Long,
                           operationType: HiveAcidOperation.OperationType,
                           acidTableMetadata: HiveAcidMetadata,
                           partitionNames: Seq[String]): Unit = synchronized {

    def createLockRequest() = {
      val requestBuilder = new LockRequestBuilder(HiveAcidDataSource.NAME)
      requestBuilder.setUser(user)
      requestBuilder.setTransactionId(txnId)
      def addLockTypeToLockComponentBuilder(
                                             lcb: LockComponentBuilder): LockComponentBuilder = {
        operationType match {
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
            throw HiveAcidErrors.invalidOperationType(operationType.toString)
        }
      }
      if (partitionNames.isEmpty) {
        val lockCompBuilder = new LockComponentBuilder()
          .setDbName(acidTableMetadata.dbName)
          .setTableName(acidTableMetadata.tableName)

        requestBuilder.addLockComponent(addLockTypeToLockComponentBuilder(lockCompBuilder).build)
      } else {
        partitionNames.foreach(partName => {
          val lockCompBuilder = new LockComponentBuilder()
            .setPartitionName(partName)
            .setDbName(acidTableMetadata.dbName)
            .setTableName(acidTableMetadata.tableName)
          requestBuilder.addLockComponent(addLockTypeToLockComponentBuilder(lockCompBuilder).build)
        })
      }
      requestBuilder.build
    }

    def lock(lockReq: LockRequest): Unit = {
      var nextSleep = 50L
      /* MAX_SLEEP is the max time each backoff() will wait for, thus the total time to wait for
      successful lock acquisition is approximately (see backoff()) maxNumWaits * MAX_SLEEP.
       */
      val defaultMaxSleep = hiveConf.getTimeVar(
        HiveConf.ConfVars.HIVE_LOCK_SLEEP_BETWEEN_RETRIES, TimeUnit.MILLISECONDS)
      val MAX_SLEEP = Math.max(15000, defaultMaxSleep)
      val maxNumWaits: Int = Math.max(0, hiveConf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_NUMRETRIES))
      def backoff(): Unit = {
        nextSleep *= 2
        if (nextSleep > MAX_SLEEP) nextSleep = MAX_SLEEP
        try
          Thread.sleep(nextSleep)
        catch {
          case e: InterruptedException =>

        }
      }
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

    lock(createLockRequest())
  }

  private class HeartbeatRunnable() extends Runnable {
    private def send(txn: HiveAcidFullTxn): Unit = {
      try {
        // Does not matter if txn is already ended
        val resp = heartBeaterClient.heartbeatTxnRange(txn.txnId, txn.txnId)
        if (resp.getAborted.nonEmpty || resp.getNosuch.nonEmpty) {
          logError(s"Heartbeat failure for transaction id: ${txn.txnId} : ${resp.toString}." +
            s"Aborting...")
          txn.setAbort
        } else {
          logDebug(s"Heartbeat sent for txnId: ${txn.txnId}")
        }
      } catch {
        // No action required because if heartbeat doesn't go for some time, transaction will be
        // aborted by HMS automatically. We can abort the transaction here also if we are not
        // able to send heartbeat for some time
        case e: TException =>
          logWarning(s"Failure to heartbeat for txnId: ${txn.txnId}", e)
      }
    }

    override def run(): Unit = {
      if (activeTxns.nonEmpty) {
        activeTxns.foreach {
          case (_, txn) if !shutdownInitiated => send(txn)
        }
      }
    }
  }

  class HeartBeaterThread(val target: Runnable, val name: String) extends Thread(target, name) {
    setDaemon(true)
  }


}
