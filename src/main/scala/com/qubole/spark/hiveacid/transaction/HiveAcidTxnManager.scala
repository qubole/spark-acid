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

package com.qubole.spark.hiveacid.transaction

import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import com.qubole.shaded.hadoop.hive.common.{ValidTxnList, ValidTxnWriteIdList, ValidWriteIdList}
import com.qubole.shaded.hadoop.hive.conf.HiveConf
import com.qubole.shaded.hadoop.hive.metastore.api.{DataOperationType, LockRequest, LockResponse, LockState}
import com.qubole.shaded.hadoop.hive.metastore.conf.MetastoreConf
import com.qubole.shaded.hadoop.hive.metastore.txn.TxnUtils
import com.qubole.shaded.hadoop.hive.metastore.{HiveMetaStoreClient, LockComponentBuilder, LockRequestBuilder}
import com.qubole.spark.hiveacid.datasource.HiveAcidDataSource
import com.qubole.spark.hiveacid.hive.HiveConverter
import com.qubole.spark.hiveacid.{HiveAcidErrors, HiveAcidOperation}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SqlUtils
import com.qubole.shaded.thrift.TException
import scala.collection.JavaConversions._
import scala.language.implicitConversions

/**
 * Txn Manager for hive acid tables.
 * This takes care of creating txns, acquiring locks and sending heartbeats
 * @param sparkSession - Spark session
 */
private[hiveacid] class HiveAcidTxnManager(sparkSession: SparkSession) extends Logging {

  private val hiveConf = HiveConverter.getHiveConf(sparkSession.sparkContext)

  private val heartbeatInterval = MetastoreConf.getTimeVar(hiveConf,
    MetastoreConf.ConfVars.TXN_TIMEOUT, TimeUnit.MILLISECONDS) / 2

  private lazy val client: HiveMetaStoreClient = new HiveMetaStoreClient(
    hiveConf, null, false)

  private lazy val heartBeaterClient: HiveMetaStoreClient =
    new HiveMetaStoreClient(hiveConf, null, false)

  // FIXME: Use thread pool so that we don't create multiple threads
  private val heartBeater: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      def newThread(r: Runnable) = new HeartBeaterThread(r, "AcidDataSourceHeartBeater")
    })
  heartBeater.scheduleAtFixedRate(
    new HeartbeatRunnable(),
    0,
    heartbeatInterval,
    TimeUnit.MILLISECONDS)

  private val user: String = sparkSession.sparkContext.sparkUser

  private val shutdownInitiated: AtomicBoolean = new AtomicBoolean(true)

  /**
    * Register transactions with Hive Metastore and tracks it under HiveAcidTxnManager.activeTxns
    * @param txn transaction which needs to begin.
    * @return Update transaction object
    */
  def beginTxn(txn: HiveAcidTxn): Long = synchronized {
    // 1. Open transaction
    val txnId = client.openTxn(HiveAcidDataSource.NAME)
    if (HiveAcidTxnManager.activeTxns.contains(txnId)) {
      throw HiveAcidErrors.repeatedTxnId(txnId, HiveAcidTxnManager.activeTxns.keySet.toSeq)
    }
    HiveAcidTxnManager.activeTxns.put(txnId, txn)
    logDebug(s"Adding txnId: $txnId to tracker")
    txnId
  }

  /**
    * Wrapper over call to hive metastore to end transaction either with commit or abort.
    * @param txnId id of transaction to be end
    * @param abort true if transaction is to be aborted.
    */
  def endTxn(txnId: Long, abort: Boolean = false): Unit = synchronized {
    try {
      // NB: Remove it from tracking before making HMS call
      // which can potentially fail.
      HiveAcidTxnManager.activeTxns.remove(txnId)
      logDebug(s"Removing txnId: $txnId from tracker")
      if (abort) {
        client.abortTxns(scala.collection.JavaConversions.seqAsJavaList(Seq(txnId)))
      } else {
        client.commitTxn(txnId)
      }
    } catch {
      case e: Exception =>
        logError(s"Failure to end txn: $txnId, presumed abort", e)
    }
  }

  /**
    * Destroy the transaction object. Closes all the pooled connection,
    * stops heartbeat and aborts all running transactions.
    */
  def close(): Unit = synchronized {

    if (!shutdownInitiated.compareAndSet(false, true)) {
      return
    }

    // Stop the heartbeat executor
    if (heartBeater != null) {
      heartBeater.shutdown()
    }

    // NB: caller of close itself is from heartbeater thread
    //    such await would be self deadlock.
    // heartBeater.awaitTermination(10, TimeUnit.SECONDS)

    // abort all active transactions
    HiveAcidTxnManager.activeTxns.foreach {
      case (_, txn) => txn.end(true)
    }
    HiveAcidTxnManager.activeTxns.clear()

    // close all clients
    if (client != null) {
      client.close()
    }
    if (heartBeaterClient != null) {
      heartBeaterClient.close()
    }
  }

  /**
    * Returns current write id.
    * @param txnId transaction id for which current write id is requested.
    * @param dbName: Database name
    * @param tableName: Table name
    * @return
    */
  def getCurrentWriteId(txnId: Long, dbName: String, tableName: String): Long = synchronized {
    client.allocateTableWriteId(txnId, dbName, tableName)
  }

  /**
    * Return list of valid txn list.
    * @param txnIdOpt txn id, current if None is passed.
    * @return
    */
  def getValidTxns(txnIdOpt: Option[Long]): ValidTxnList = synchronized {
    txnIdOpt match {
      case Some(id) => client.getValidTxns(id)
      case None => client.getValidTxns()
    }
  }

  /**
    * Return list of all valid write ids for the table.
    * @param fullyQualifiedTableName name of the table
    * @param validTxnList valid txn list snapshot.
    * @return List of valid write ids
    */
  def getValidWriteIds(validTxnList: ValidTxnList,
                        fullyQualifiedTableName: String): ValidWriteIdList = synchronized {
    getValidWriteIds(None, validTxnList, fullyQualifiedTableName)
  }

  /**
    * Return list of all valid write ids for the table for given transactions
    * @param txnId transaction id
    * @param validTxnList valid txn list snapshot.
    * @param fullyQualifiedTableName table name
    * @return List of valid write ids
    */
  def getValidWriteIds(txnId: Long,
                       validTxnList: ValidTxnList,
                       fullyQualifiedTableName: String): ValidWriteIdList = synchronized {
    getValidWriteIds(Option(txnId), validTxnList, fullyQualifiedTableName)
  }

  private def getValidWriteIds(txnIdOpt: Option[Long],
                               validTxnList: ValidTxnList,
                               fullyQualifiedTableName: String): ValidWriteIdList = synchronized {
    val txnId = txnIdOpt match {
      case Some(id) => id
      case None => -1L
    }
    val tableValidWriteIds = client.getValidWriteIds(Seq(fullyQualifiedTableName),
      validTxnList.writeToString())
    val txnWriteIds: ValidTxnWriteIdList = TxnUtils.createValidTxnWriteIdList(txnId,
      tableValidWriteIds)
    txnWriteIds.getTableValidWriteIdList(fullyQualifiedTableName)
  }

  /**
    * API to acquire locks on partitions
    * @param txnId transaction id
    * @param dbName: Database name
    * @param tableName: Table name
    * @param operationType lock type
    * @param partitionNames partition names
    */
  def acquireLocks(txnId: Long,
                   dbName: String,
                   tableName: String,
                   operationType: HiveAcidOperation.OperationType,
                   partitionNames: Seq[String]): Unit = synchronized {

    // Consider following sequence of event
    //  T1:   R(x)
    //  T2:   R(x)
    //  T2:   W(x)
    //  T2:   Commit
    //  T1:   W(x)
    // Because read happens with MVCC it is possible that some other transaction
    // may have come and performed write. To protect against the lost write due
    // to above sequence hive maintains write-set and abort conflict transaction
    // optimistically at the commit time.
    def addLockType(lcb: LockComponentBuilder): LockComponentBuilder = {
      operationType match {
        case HiveAcidOperation.INSERT_OVERWRITE =>
          lcb.setExclusive().setOperationType(DataOperationType.UPDATE)
        case HiveAcidOperation.INSERT_INTO =>
          lcb.setShared().setOperationType(DataOperationType.INSERT)
        case HiveAcidOperation.READ =>
          lcb.setShared().setOperationType(DataOperationType.SELECT)
        case HiveAcidOperation.UPDATE =>
          lcb.setSemiShared().setOperationType(DataOperationType.UPDATE)
        case HiveAcidOperation.DELETE =>
          lcb.setSemiShared().setOperationType(DataOperationType.DELETE)
        case _ =>
          throw HiveAcidErrors.invalidOperationType(operationType.toString)
      }
    }

    def createLockRequest() = {
      val requestBuilder = new LockRequestBuilder(HiveAcidDataSource.NAME)
      requestBuilder.setUser(user)
      requestBuilder.setTransactionId(txnId)
     if (partitionNames.isEmpty) {
        val lockCompBuilder = new LockComponentBuilder()
          .setDbName(dbName)
          .setTableName(tableName)

        requestBuilder.addLockComponent(addLockType(lockCompBuilder).build)
      } else {
        partitionNames.foreach(partName => {
          val lockCompBuilder = new LockComponentBuilder()
            .setPartitionName(partName)
            .setDbName(dbName)
            .setTableName(tableName)
          requestBuilder.addLockComponent(addLockType(lockCompBuilder).build)
        })
      }
      requestBuilder.build
    }

    def lock(lockReq: LockRequest): Unit = {
      var nextSleep = 50L

      // FIXME: This is crazy long wait for locks. Sleep starts from 50ms and
      //  exponentially in power of 2 backs off to MAX_SLEEP the maximum sleep
      //  for unsuccessful lock acquisition time is maxNumWaits * MAX_SLEEP,
      //  which defaults to 60s * 100 that is 6000s that is 2hours.
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
          case _: InterruptedException =>

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
    private def send(txn: HiveAcidTxn): Unit = {
      try {
        // Does not matter if txn is already ended
        val resp = heartBeaterClient.heartbeatTxnRange(txn.txnId, txn.txnId)
        if (resp.getAborted.nonEmpty || resp.getNosuch.nonEmpty) {
          logError(s"Heartbeat failure for transaction id: ${txn.txnId} : ${resp.toString}." +
            s"Aborting...")
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
      if (shutdownInitiated.get()) {
        return
      }

      // Close the txnManager
      if (SqlUtils.hasSparkStopped(sparkSession)) {
        close()
        return
      }

      if (HiveAcidTxnManager.activeTxns.nonEmpty) {
        HiveAcidTxnManager.activeTxns.foreach {
          case (_, txn) => send(txn)
        }
     }
    }
  }

  class HeartBeaterThread(val target: Runnable, val name: String) extends Thread(target, name) {
    setDaemon(true)
  }
}

protected[hiveacid] object HiveAcidTxnManager {
  // Maintain activeTxns inside txnManager instead of HiveAcidTxn
  // object for it to be accessible to back ground thread running
  // inside HiveAcidTxnManager.
  protected val activeTxns = new scala.collection.mutable.HashMap[Long, HiveAcidTxn]()
  def getTxn(txnId: Long): Option[HiveAcidTxn] = HiveAcidTxnManager.activeTxns.get(txnId)
}
