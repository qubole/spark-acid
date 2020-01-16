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

import java.util.concurrent.atomic.AtomicBoolean

import com.qubole.shaded.hadoop.hive.common.{ValidTxnList, ValidWriteIdList}
import com.qubole.spark.hiveacid.{HiveAcidErrors, HiveAcidOperation}
import com.qubole.spark.hiveacid.hive.HiveAcidMetadata

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * Hive Acid Transaction object.
  * @param sparkSession: Spark Session
  */
class HiveAcidTxn(sparkSession: SparkSession) extends Logging {

  HiveAcidTxn.setUpTxnManager(sparkSession)

  // txn ID
  protected var id: Long = -1
  protected var validTxnList: ValidTxnList = _
  private [hiveacid] val isClosed: AtomicBoolean = new AtomicBoolean(true)

  private def setTxn(id: Long, txns:ValidTxnList): Unit = {
    this.id = id
    this.validTxnList = txns
    isClosed.set(false)
  }

  private def unsetTxn(): Unit = {
    this.id = -1
    this.validTxnList = null
    isClosed.set(true)
  }

  override def toString: String = s"""{"id":"$id","validTxns":"$validTxnList"}"""

  /**
    * Public API to being transaction.
    */
  def begin(): Unit = synchronized {
    if (!isClosed.get) {
      throw HiveAcidErrors.txnAlreadyOpen(id)
    }
    val newId = HiveAcidTxn.txnManager.beginTxn(this)
    val txnList = HiveAcidTxn.txnManager.getValidTxns(Some(newId))
    setTxn(newId, txnList)
    // Set it for thread for all future references.
    HiveAcidTxn.threadLocal.set(this)
    logDebug(s"Begin transaction $this")
  }

  /**
    * Public API to end transaction
    * @param abort true if transaction is aborted
    */
  def end(abort: Boolean = false): Unit = synchronized {
    if (isClosed.get) {
      throw HiveAcidErrors.txnAlreadyClosed(id)
    }

    logDebug(s"End transaction $this abort = $abort")
    // NB: Unset it for thread proactively invariant of
    //  underlying call fails or succeeds.
    HiveAcidTxn.threadLocal.set(null)
    HiveAcidTxn.txnManager.endTxn(id, abort)
    unsetTxn()
  }

  private[hiveacid] def acquireLocks(hiveAcidMetadata: HiveAcidMetadata,
                                     operationType: HiveAcidOperation.OperationType,
                                     partitionNames: Seq[String]): Unit = {
    if (isClosed.get()) {
      logError(s"Transaction already closed $this")
      throw HiveAcidErrors.txnAlreadyClosed(id)
    }
    HiveAcidTxn.txnManager.acquireLocks(id, hiveAcidMetadata.dbName,
      hiveAcidMetadata.tableName, operationType, partitionNames)
  }
  // Public Interface
  def txnId: Long = id
}

object HiveAcidTxn extends Logging {

  val threadLocal = new ThreadLocal[HiveAcidTxn]

  // Helper function to create snapshot.
  private[hiveacid] def createSnapshot(txn: HiveAcidTxn, hiveAcidMetadata: HiveAcidMetadata): HiveAcidTableSnapshot = {
    val currentWriteId = txnManager.getCurrentWriteId(txn.txnId,
      hiveAcidMetadata.dbName, hiveAcidMetadata.tableName)
    val validWriteIdList = if (txn.txnId == - 1) {
      throw HiveAcidErrors.tableWriteIdRequestedBeforeTxnStart (hiveAcidMetadata.fullyQualifiedName)
    } else {
      txnManager.getValidWriteIds(txn.txnId, txn.validTxnList ,hiveAcidMetadata.fullyQualifiedName)
    }
    HiveAcidTableSnapshot(validWriteIdList, currentWriteId)
  }

  // Txn manager is connection to HMS. Use single instance of it
  var txnManager: HiveAcidTxnManager = _
  private def setUpTxnManager(sparkSession: SparkSession): Unit = synchronized {
    if (txnManager == null) {
      txnManager = new HiveAcidTxnManager(sparkSession)
    }
  }

  /**
    * Creates read or write transaction based on user request.
    *
    * @param sparkSession Create a new hive Acid transaction
    * @return
    */
  def createTransaction(sparkSession: SparkSession): HiveAcidTxn = {
    setUpTxnManager(sparkSession)
    new HiveAcidTxn(sparkSession)
  }

  /**
    * Given a transaction id return the HiveAcidTxn object. Raise exception if not found.
    * @return
    */
  def currentTxn(): HiveAcidTxn = {
    threadLocal.get()
  }
}

private[hiveacid] case class HiveAcidTableSnapshot(validWriteIdList: ValidWriteIdList, currentWriteId: Long)
