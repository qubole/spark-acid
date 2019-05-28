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
import org.apache.hadoop.conf.Configuration
import com.qubole.shaded.hive.conf.HiveConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, FileIndex, FileStatusCache, InMemoryFileIndex, PartitionDirectory, PartitionPath, PartitionSpec, PrunedInMemoryFileIndex}
import com.qubole.shaded.hive.metastore.HiveMetaStoreClient
import com.qubole.shaded.hive.metastore.api.{DataOperationType, HeartbeatTxnRangeRequest, LockRequest, LockResponse, LockState, MetaException, Table, TableValidWriteIds}
import com.qubole.shaded.hive.metastore.txn.TxnUtils
import com.qubole.shaded.hive.ql.lockmgr.LockException
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, GenericInternalRow, Literal}
import org.apache.thrift.TException
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}

import com.qubole.shaded.hive.common.ValidWriteIdList
import com.qubole.shaded.hive.metastore.{LockComponentBuilder, LockRequestBuilder}
import org.apache.spark.sql.execution.{QueryExecution, RowDataSourceScanExec}
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.JavaConversions._

class HiveAcidState(sparkSession: SparkSession,
                    val hiveConf: HiveConf,
                    val table: Table,
                    val sizeInBytes: Long,
                    val client: HiveMetaStoreClient,
                    val pSchema: StructType,
                    val heartbeatInterval: Long,
                    val isFullAcidTable: Boolean) extends Logging {

  val user: String = sparkSession.sparkContext.sparkUser
  val dbName: String = table.getDbName
  val tableName: String = table.getTableName
  val location: Path = new Path(table.getSd.getLocation)
  var txnId: Long = -1
  var validWriteIds: ValidTxnWriteIdList = _
  var isTxnClosed = false
  var heartBeater: ScheduledExecutorService = _

  var nextSleep: Long = _
  var MAX_SLEEP: Long = _




//  def rootPath: Seq[Path] = Seq(new Path(table.getSd.getLocation)) //.map(new Path(_)).toSeq
//
//  private val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
//
//  override def refresh(): Unit = fileStatusCache.invalidateAll()
//
//  override def partitionSchema: StructType = pSchema
//
//  override def inputFiles: Array[String] = new InMemoryFileIndex(
//    sparkSession, rootPaths, Option(table.getSd.getSerdeInfo.getParameters)
//      .map(_.toMap).orNull, userSpecifiedSchema = None).inputFiles
//
//  override def listFiles(
//                          partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
//    if (_txnId == -1) {
//      _txnId = client.openTxn("HiveAcidDataSource")
//    }
//    // Somani: Need to take care partitioning here
//    var validTxns = client.getValidTxns(_txnId)
//    var validWriteIds = client.getValidWriteIds(Seq(table.getDbName + "." + table.getTableName), client.getValidTxns(_txnId).writeToString())
//    new InMemoryFileIndex(
//      sparkSession, rootPaths, Option(table.getSd.getSerdeInfo.getParameters)
//        .map(_.toMap).orNull, userSpecifiedSchema = None).listFiles(Nil, dataFilters)
//  }

  def close(): Unit = {
    if (txnId != -1 && !isTxnClosed) {
      println("Somani job end, closing _acid_state txn id: " + txnId)
      client.commitTxn(txnId)
      txnId = -1
      isTxnClosed = true
      heartBeater.shutdown()
      heartBeater = null
    } else {
      logWarning("Transaction already closed")
    }
  }

  def acquireLocks(partitionNames: Seq[String] = null): Unit = {
    if (isTxnClosed || (txnId == -1)) {
      logWarning("Transaction already closed")
      //TODO: throw exception
      return
    }
    val req: LockRequest = createLockRequest(partitionNames)
    lock(req)
  }

  lazy val getValidWriteIds: ValidWriteIdList = {
    val validTxns = client.getValidTxns(txnId)
    //validWriteIds = TxnUtils.createValidTxnWriteIdList(txnId, client.getValidWriteIds(Seq(dbName + "." + tableName),
    //validTxns.writeToString()))
    val txnWriteIds: ValidTxnWriteIdList = TxnUtils.createValidTxnWriteIdList(txnId, client.getValidWriteIds(Seq(dbName + "." + tableName),
      validTxns.writeToString()))
    // store in conf txnWriteIds.toString()
    //val writeIdStr = txnWriteIds.toString();
    //    conf.set(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY, writeIdStr);
    //val validTxnList: ValidTxnWriteIdList = new ValidTxnWriteIdList(writeIdStr)
    txnWriteIds.getTableValidWriteIdList(table.getDbName  + "." + table.getTableName)
  }

  // Use this instead of open(), acquireLocks(), getValidWriteIds() and close() if not doing transaction management.
  lazy val getValidWriteIdsNoTxn: ValidWriteIdList = {
    val validTxns = client.getValidTxns()
    //validWriteIds = TxnUtils.createValidTxnWriteIdList(txnId, client.getValidWriteIds(Seq(dbName + "." + tableName),
    //validTxns.writeToString()))
    val txnWriteIds: ValidTxnWriteIdList = TxnUtils.createValidTxnWriteIdList(txnId, client.getValidWriteIds(Seq(dbName + "." + tableName),
      validTxns.writeToString()))
    // store in conf txnWriteIds.toString()
    //val writeIdStr = txnWriteIds.toString();
    //    conf.set(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY, writeIdStr);
    //val validTxnList: ValidTxnWriteIdList = new ValidTxnWriteIdList(writeIdStr)
    txnWriteIds.getTableValidWriteIdList(table.getDbName  + "." + table.getTableName)
  }

  def open(): Unit = {
    if (txnId == -1) {
      // 1. Open transaction
      txnId = client.openTxn(HiveAcidDataSource.agentName) // TODO change this to user instead
      println("Somani opened txnid: " + txnId)
      isTxnClosed = false
      // 2. Start HeartBeater
      if (heartBeater == null) {
        heartBeater = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
          def newThread(r: Runnable) = new HeartBeaterThread(r, "AcidDataSourceHeartBeater")
        })
      } else {
        //TODO: throw exception
      }

      heartBeater.scheduleAtFixedRate(new HeartbeatRunnable(),
        (heartbeatInterval * 0.75 * Math.random()).asInstanceOf[Long],
        heartbeatInterval,
        TimeUnit.MILLISECONDS)
      registerQEListener(sparkSession.sqlContext)
    } else {
      //TODO: Throw and exception here
      logWarning("Not opening a transaction as transaction with id " + txnId + " already open")
    }
  }

  private class HeartbeatRunnable() extends Runnable {
    override def run(): Unit = {
      try {
        if (txnId > 0 && !isTxnClosed) {
          val resp = client.heartbeatTxnRange(txnId, txnId)
          if (!resp.getAborted.isEmpty || !resp.getNosuch.isEmpty) {
            logError("Heartbeat failure: " + resp.toString)
            isTxnClosed = true
            heartBeater.shutdown()
            heartBeater = null
          } else {
            logInfo("Heartbeat sent for txnId: " + txnId)
          }
        }
      }
      catch {
        case e: TException =>
          logWarning("Failure to heartbeat for txnId: " + txnId)
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
    if (partNames == null) {
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

  def lock(lockReq: LockRequest): Unit = {
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
        //throw exception
      }
    } catch {
      case e: TException =>
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
        close()
        //sqlContext.sparkSession.listenerManager.unregister(this)
      }

      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        close()
        //compareAndClose(qe)
        //sqlContext.sparkSession.listenerManager.unregister(this)
      }
    })
  }

  private def compareAndClose(qe: QueryExecution): Unit = {
    val acidStates = qe.executedPlan.collect {
      case RowDataSourceScanExec(_, _, _, _, _, relation: HiveAcidRelation, _)
        if relation.acidState == this =>
        relation.acidState
    }.filter(_ != null)
    acidStates.foreach(_.close())
  }

//  def getValidWriteIds()
}
