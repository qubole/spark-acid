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

import java.util.concurrent.atomic.AtomicBoolean

import com.qubole.shaded.hadoop.hive.common.ValidWriteIdList
import com.qubole.spark.datasources.hiveacid.{HiveAcidErrors, HiveAcidMetadata, HiveAcidOperation}
import org.apache.spark.internal.Logging

private[hiveacid] abstract class HiveAcidTxn(
    val acidTableMetadata: HiveAcidMetadata,
    hiveAcidTxnManager: HiveAcidTxnManager) extends Logging {

  def begin(): Unit = {}

  def setAbort(): Unit = {}

  def isAborted(): Boolean = false

  def end(abort: Boolean = false): Unit = {}

  def acquireLocks(operationType: HiveAcidOperation.OperationType,
                   partitionNames: Seq[String]): Unit = {}

  val currentWriteId: Long

  val validWriteIds: ValidWriteIdList
}

// Special Txn for transaction less operations
private[hiveacid] class HiveAcidReadTxn(override val acidTableMetadata: HiveAcidMetadata,
                                        txnManager: HiveAcidTxnManager)
  extends HiveAcidTxn(acidTableMetadata, txnManager) {
  override lazy val currentWriteId: Long = throw HiveAcidErrors.operationNotSupported
  override lazy val validWriteIds: ValidWriteIdList = txnManager.getValidWriteIds(
    acidTableMetadata.fullyQualifiedName)
}

private[hiveacid] class HiveAcidFullTxn(override val acidTableMetadata: HiveAcidMetadata,
                                        txnManager: HiveAcidTxnManager)
  extends HiveAcidTxn(acidTableMetadata, txnManager) {

  private var id: Long = -1
  private var shouldAbort = false
  private val isClosed: AtomicBoolean = new AtomicBoolean(false)

  def txnId: Long = id

  override def begin(): Unit = {
    if (id != -1) {
      throw HiveAcidErrors.txnAlreadyOpen(id)
    }
    id = txnManager.beginTxn(this)
  }

  override def setAbort: Unit = {
    shouldAbort = true
  }

  override def isAborted: Boolean = shouldAbort

  override def end(abort: Boolean = false): Unit = {
    if (isClosed.compareAndSet(false, true)) {
      val doAbort = abort || shouldAbort
      logInfo(s"Closing transaction $txnId on table " +
        s"${acidTableMetadata.fullyQualifiedName}. abort = $doAbort")
      txnManager.endTxn(txnId, doAbort)
    } else {
      throw HiveAcidErrors.txnAlreadyClosed(txnId)
    }
  }

  override def acquireLocks(operationType: HiveAcidOperation.OperationType,
                            partitionNames: Seq[String]): Unit = {
    if (isClosed.get() || shouldAbort) {
      logError("Transaction already closed")
      throw HiveAcidErrors.txnAlreadyClosed(txnId)
    }
    txnManager.acquireLocks(txnId, operationType,
      acidTableMetadata, partitionNames)
  }

  override lazy val currentWriteId: Long = txnManager.getCurrentWriteId(txnId, acidTableMetadata)

  override lazy val validWriteIds: ValidWriteIdList = {
    if (id == -1) {
      throw HiveAcidErrors.tableWriteIdRequestedBeforeTxnStart(acidTableMetadata.fullyQualifiedName)
    }
    txnManager.getValidWriteIds(txnId, acidTableMetadata.fullyQualifiedName)
  }
}
