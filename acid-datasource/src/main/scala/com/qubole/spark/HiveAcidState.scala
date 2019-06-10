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

package com.qubole.spark

import com.qubole.shaded.hive.common.ValidTxnWriteIdList
import com.qubole.shaded.hive.conf.HiveConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import com.qubole.shaded.hive.metastore.HiveMetaStoreClient
import com.qubole.shaded.hive.metastore.api.{DataOperationType, LockRequest, LockResponse, LockState, Table}
import com.qubole.shaded.hive.metastore.txn.TxnUtils
import org.apache.thrift.TException
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}

import com.qubole.shaded.hive.common.ValidWriteIdList
import com.qubole.shaded.hive.metastore.{LockComponentBuilder, LockRequestBuilder}
import com.qubole.spark.util.Util
import com.qubole.shaded.hive.ql.metadata
import com.qubole.spark.rdd.AcidLockUnionRDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{QueryExecution, RowDataSourceScanExec}
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class HiveAcidState(sparkSession: SparkSession,
                    val hiveConf: HiveConf,
                    val table: metadata.Table,
                    val sizeInBytes: Long,
                    val pSchema: StructType,
                    val heartbeatInterval: Long,
                    val isFullAcidTable: Boolean) extends Logging {

  private val user: String = sparkSession.sparkContext.sparkUser
  private var _client: HiveMetaStoreClient = _
  private val dbName: String = table.getDbName
  private val tableName: String = table.getTableName
  private var txnId: Long = -1
  private var isTxnClosed = false
  private var heartBeater: ScheduledExecutorService = _
  private lazy val heartBeaterClient: HiveMetaStoreClient = new HiveMetaStoreClient(hiveConf,null, false)
  private var nextSleep: Long = _
  private var MAX_SLEEP: Long = _

  val location: Path = table.getDataLocation

  def begin(partitionNames: Seq[String]): Unit = {
    if (txnId != -1) {
      throw HiveAcidErrors.txnAlreadyOpen(txnId)
    }
    // 1. Open transaction
    txnId = client.openTxn(HiveAcidDataSource.agentName) // TODO change this to user instead
    logInfo("Opened txnid: " + txnId + " for table " + dbName + "." + tableName)
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
    registerQEListener(sparkSession.sqlContext)
    // 3. Acquire locks
    acquireLocks(partitionNames)
  }

  def end(): Unit = {
    if (txnId != -1 && !isTxnClosed) {
      synchronized {
        if (txnId != -1 && !isTxnClosed) {
          try {
            logInfo("Closing txnid: " + txnId + " for table " + dbName + "." + tableName)
            client.commitTxn(txnId)
            closeClient()
            txnId = -1
            isTxnClosed = true
            heartBeater.shutdown() //TODO: is this ok to call from here?
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
      client.getValidWriteIds(Seq(dbName + "." + tableName),
        validTxns.writeToString()))
    txnWriteIds.getTableValidWriteIdList(table.getDbName  + "." + table.getTableName)
  }

  /* Unused for now.
   * Use this instead of open(), acquireLocks(), getValidWriteIds() and close() if not doing transaction management.
   */
  lazy val getValidWriteIdsNoTxn: ValidWriteIdList = {
    val validTxns = client.getValidTxns()
    val txnWriteIds: ValidTxnWriteIdList = TxnUtils.createValidTxnWriteIdList(txnId,
      client.getValidWriteIds(Seq(dbName + "." + tableName),
        validTxns.writeToString()))
    txnWriteIds.getTableValidWriteIdList(table.getDbName  + "." + table.getTableName)
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
    val requestBuilder = new LockRequestBuilder(HiveAcidDataSource.agentName)
    requestBuilder.setUser(user)
    requestBuilder.setTransactionId(txnId)
    if (partNames.isEmpty) {
      val lockCompBuilder = new LockComponentBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .setShared()
        .setOperationType(DataOperationType.SELECT)
      requestBuilder.addLockComponent(lockCompBuilder.build)
    } else {
      partNames.foreach(x =>  {
        val lockCompBuilder = new LockComponentBuilder()
          .setPartitionName(x)
          .setDbName(dbName)
          .setTableName(tableName)
          .setShared()
          .setOperationType(DataOperationType.SELECT)
        requestBuilder.addLockComponent(lockCompBuilder.build)
      })
    }
    requestBuilder.build
  }

  private def lock(lockReq: LockRequest): Unit = {
    nextSleep = 50
    /* MAX_SLEEP is the max time each backoff() will wait for, thus the total time to wait for
    successful lock acquisition is approximately (see backoff()) maxNumWaits * MAX_SLEEP.
     */
    MAX_SLEEP = Math.max(15000, hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_LOCK_SLEEP_BETWEEN_RETRIES, TimeUnit.MILLISECONDS))
    val maxNumWaits: Int = Math.max(0, hiveConf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_NUMRETRIES))
    try {
      logInfo("Requesting lock : " + lockReq)
      var res: LockResponse = client.lock(lockReq)
      //link lockId to queryId
      logInfo("Lock response: " + res)
      var numRetries: Int = 0
      while (res.getState == LockState.WAITING && numRetries < maxNumWaits) {
        numRetries += 1
        backoff()
        res = client.checkLock(res.getLockid)
      }
      if (res.getState != LockState.ACQUIRED) {
        throw HiveAcidErrors.couldNotAcquireLockException()
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

  private def registerQEListener(sqlContext: SQLContext): Unit = {
    sqlContext.sparkSession.listenerManager.register(new QueryExecutionListener {
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        compareAndClose(qe)
        Future {
          // Doing this in a Future as both unregister and onSuccess take the same lock
          // to avoid modification of the queue while it is being processed
          sqlContext.sparkSession.listenerManager.unregister(this)
          logDebug(s"listener unregistered for ${HiveAcidState.this}")
        }
      }

      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        compareAndClose(qe)
        Future {
          // Doing this in a Future as both unregister and onSuccess take the same lock
          // to avoid modification of the queue while it is being processed
          sqlContext.sparkSession.listenerManager.unregister(this)
          logDebug(s"listener unregistered for ${HiveAcidState.this}")
        }
      }
    })
  }

  private def compareAndClose(qe: QueryExecution): Unit = {
    val acidStates = qe.executedPlan.collect {
      case RowDataSourceScanExec(_, _, _, _, rdd: AcidLockUnionRDD[InternalRow],
                                 _, _) if rdd.acidState == this =>
        rdd.acidState
    }
    acidStates.foreach(_.end())
  }
}
