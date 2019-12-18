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

package com.qubole.spark.datasources.hiveacid

import java.util.{ArrayList, List}

import com.qubole.shaded.hadoop.hive.common.{ValidTxnWriteIdList, ValidWriteIdList}
import com.qubole.shaded.hadoop.hive.conf.HiveConf
import com.qubole.shaded.hadoop.hive.metastore.HiveMetaStoreClient
import com.qubole.shaded.hadoop.hive.metastore.txn.TxnUtils
import com.qubole.shaded.hadoop.hive.metastore.api
import com.qubole.shaded.hadoop.hive.ql.metadata
import com.qubole.spark.datasources.hiveacid
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import com.qubole.shaded.hadoop.hive.ql.lockmgr.DbLockManager

import scala.collection.JavaConversions._
import java.util.concurrent._

import com.qubole.spark.datasources.hiveacid.rdd.{SparkAcidListener, SparkAcidQueryListener}
import com.qubole.shaded.hadoop.hive.metastore.{LockComponentBuilder, LockRequestBuilder}
import com.qubole.shaded.hadoop.hive.metastore.api.{DataOperationType, LockComponent}
import com.qubole.shaded.hadoop.hive.ql.lockmgr.HiveLock
import com.qubole.shaded.hadoop.hive.metastore.api.LockResponse

class HiveAcidState(sparkSession: SparkSession,
                    val hiveConf: HiveConf,
                    val sizeInBytes: Long,
                    val pSchema: StructType,
                    val isFullAcidTable: Boolean) extends Logging {
  private var dbName: String = _
  private var tableName: String = _
  private var txnId: Long = -1
  private var lockId: Long = -1
  private var validWriteIds: ValidWriteIdList = _
  private var heartBeater: ScheduledFuture[_] = _
  sparkSession.sparkContext.addSparkListener(new SparkAcidListener(this))
  sparkSession.listenerManager.register(new SparkAcidQueryListener(this))
  private val sparkUser = sparkSession.sparkContext.sparkUser

  def startHeartbitThread(txnId : Long) : ScheduledFuture[_] = {
    // Need to create a new client as multi thread is not supported for normal hms client.
    val client = new HiveMetaStoreClient(hiveConf, null, false)

    val ex = new ScheduledThreadPoolExecutor(1)
    val task = new Runnable {
      def run(): Unit = client.heartbeat(txnId, lockId)
    }
    ex.scheduleAtFixedRate(task, 1, 1, TimeUnit.SECONDS)
  }

  def addTable(table: metadata.Table) : Unit = {
    dbName = table.getDbName
    tableName = table.getTableName
  }

  def lockTable(client : HiveMetaStoreClient): Unit = {
    val compBuilder = new LockComponentBuilder
    compBuilder.setShared
    compBuilder.setOperationType(DataOperationType.SELECT)
    compBuilder.setDbName(dbName)
    compBuilder.setTableName(tableName)

    val lockComponents: java.util.List[LockComponent] = new java.util.ArrayList[LockComponent]
    lockComponents.add(compBuilder.build)

    val rqstBuilder = new LockRequestBuilder("spark-acid")
    rqstBuilder.setTransactionId(txnId).setUser(sparkUser)
    //rqstBuilder.addLockComponents(lockComponents)
    lockId = client.lock(rqstBuilder.build).asInstanceOf[LockResponse].getLockid
  }

  def beginRead(): Unit = {
    val client = new HiveMetaStoreClient(hiveConf, null, false)

    try {
      txnId = client.openTxn("spark-acid")
      heartBeater = startHeartbitThread(txnId)
      lockTable(client)
      val validTxns = client.getValidTxns(txnId)
      val txnWriteIds: ValidTxnWriteIdList = TxnUtils.createValidTxnWriteIdList(txnId,
        client.getValidWriteIds(Seq(dbName + "." + tableName),
          validTxns.writeToString()))
      validWriteIds = txnWriteIds.getTableValidWriteIdList(dbName + "." + tableName)
    } catch {
      case ise: Throwable => endTxn(client)
    } finally {
      client.close()
    }
  }

  def endTxn(client : HiveMetaStoreClient): Unit = {
    if (txnId != -1) {
      heartBeater.cancel(true)
      try {
        client.commitTxn(txnId)
      } finally {
        // If heart bit thread is stopped, the transaction will be committed eventually.
        txnId = -1
      }
    }
  }

  def end(): Unit = {
    val client = new HiveMetaStoreClient(hiveConf, null, false)
    try {
      endTxn(client)
    } finally {
      client.close()
    }
  }

  def getValidWriteIds: ValidWriteIdList = {
    if (validWriteIds == null) {
      throw HiveAcidErrors.validWriteIdsNotInitialized
    }
    validWriteIds
  }
}
