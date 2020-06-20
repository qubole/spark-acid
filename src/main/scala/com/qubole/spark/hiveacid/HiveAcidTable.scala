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
import com.qubole.spark.hiveacid.datasource.HiveAcidDataSource
import com.qubole.spark.hiveacid.merge.{MergeWhenClause, MergeWhenNotInsert}
import com.qubole.spark.hiveacid.rdd.EmptyRDD
import com.qubole.spark.hiveacid.transaction._
import org.apache.spark.annotation.InterfaceStability.{Evolving}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression


/**
  * Represents a hive acid table and exposes API to perform operations on top of it
  * @param sparkSession - spark session object
  * @param hiveAcidMetadata - metadata object
  * @param parameters - additional parameters
  */
@Evolving
class HiveAcidTable(val sparkSession: SparkSession,
                    val hiveAcidMetadata: HiveAcidMetadata,
                    val parameters: Map[String, String]
                   ) extends Logging {

  private val delegate: AcidOperationDelegate =
    new HiveAcidOperationDelegate(sparkSession, hiveAcidMetadata, parameters)

  /**
    * Create dataframe to read based on hiveAcidTable and passed in filter.
    * @return Dataframe
    */
  def readDF(withRowId: Boolean): DataFrame = {
    val options = if (withRowId) {
      // Fetch row with rowID in it
      parameters ++ Map("includeRowIds" -> "true", "table" -> hiveAcidMetadata.fullyQualifiedName)
    } else {
      parameters
    }
    sparkSession.read.format(HiveAcidDataSource.NAME)
      .options(options)
      .load()
  }

  /**
    * Returns true if the table is an insert only table
    */
  def isInsertOnlyTable: Boolean = {
    hiveAcidMetadata.isInsertOnlyTable
  }

  /**
    * Return an RDD on top of Hive ACID table
    *
    * @param requiredColumns - columns needed
    * @param filters - filters that can be pushed down to file format
    * @param readConf - read conf
    * @return
    */
  @Evolving
  def getRdd(requiredColumns: Array[String],
             filters: Array[Filter],
             readConf: SparkAcidConf): RDD[Row] = {
    var res: RDD[Row] = new EmptyRDD[Row](sparkSession.sparkContext)

    // TODO: Read does not perform read but returns an RDD, which materializes
    //  outside this function. For transactional guarantees, the transaction
    //  boundary needs to span getRDD call. Currently we return the RDD
    //  without any protection.
    inTxn { txn =>
      res = delegate.getRdd(requiredColumns, filters, readConf, txn)
    }
    res
  }

  /**
    * Used by streaming query to add a datframe to hive acid table.
    * @param df - dataframe to insert
    * @return - transaction Id
    */
  def addBatch(df: DataFrame): Long = {
    var txnId = -1L
    inTxn {
      txn: HiveAcidTxn => {
        delegate.addBatch(df, txn)
        txnId = HiveAcidTxn.currentTxn().txnId
      }
    }
    txnId
  }

  private def inTxn(f: HiveAcidTxn =>  Unit): Unit = {
    new HiveTxnWrapper(sparkSession).inTxn(f)
  }

  /**
    * Appends a given dataframe df into the hive acid table
    *
    * Note: This API is transactional in nature.
    * @param df - dataframe to insert
    * @param statementId Optional. In a same transaction, multiple statements like INSERT/UPDATE/DELETE
    *                    (like in case of MERGE) can be issued.
    *                    [[statementId]] has to be different for them to ensure delta collision
    *                    is avoided for them during writes.
    */
  def insertInto(df: DataFrame, statementId: Option[Int] = None): Unit = inTxn {
    curTxn => delegate.insertInto(df, curTxn, statementId)
  }

  /**
    * Overwrites a given dataframe df onto the hive acid table
    *
    * Note: This API is transactional in nature.
    * @param df - dataframe to insert
    * @param statementId Optional. In a same transaction, multiple statements like INSERT/UPDATE/DELETE
    *                    (like in case of MERGE) can be issued.
    *                    [[statementId]] has to be different for them to ensure delta collision
    *                    is avoided for them during writes.
    */
  def insertOverwrite(df: DataFrame, statementId: Option[Int] = None): Unit = inTxn {
    curTxn => delegate.insertOverwrite(df, curTxn, statementId)
  }

  /**
    * Delete rows from the table based on `condtional` boolean expression.
    *
    * Note: This API is transactional in nature.
    * @param condition - Boolean SQL Expression filtering rows to be deleted
    */
  @Evolving
  def delete(condition: String): Unit = {
    delete(functions.expr(condition))
  }

  /**
    * Delete rows from the table based on `condtional` expression.
    *
    * Note: This API is transactional in nature.
    * @param condition - Boolean SQL Expression filtering rows to be deleted
    */
  @Evolving
  def delete(condition: Column): Unit = inTxn {
    curTxn => delegate.delete(condition, curTxn)
  }

  /**
    * Update rows in the hive acid table based on condition and newValues
    *
    * Note: This API is transactional in nature.
    * @param condition - condition string to identify rows which needs to be updated
    * @param newValues - Map of (column, value) to set
    */
  @Evolving
  def update(condition: Option[String], newValues: Map[String, String]): Unit = inTxn {
    curTxn => delegate.update(condition, newValues, curTxn)
  }
  /**
    * Update rows in the hive acid table based on condition and newValues
    *
    * Note: This API is transactional in nature.
    * @param condition - Optional condition string to identify rows which needs to be updated,
    *                  if not specified then it means complete table.
    * @param newValues - Map of (column, value) to set
    */
  @Evolving
  def update(condition: Option[Column], newValues: java.util.Map[String, Column]): Unit = inTxn {
    curTxn => delegate.update(condition, newValues, curTxn)
  }

  @Evolving
  def merge(sourceDf: DataFrame,
            mergeExpression: Expression,
            matchedClause: Seq[MergeWhenClause],
            notMatched: Option[MergeWhenNotInsert],
            sourceAlias: Option[AliasIdentifier],
            targetAlias: Option[AliasIdentifier]): Unit = inTxn {
    txn => delegate.merge(sourceDf, hiveAcidMetadata, mergeExpression, matchedClause,
      notMatched, sourceAlias, targetAlias, txn)
  }

  def isFullAcidTable: Boolean = {
    hiveAcidMetadata.isFullAcidTable
  }

  def isBucketed: Boolean = {
    hiveAcidMetadata.isBucketed
  }
}

object HiveAcidTable {
  def fromSparkSession(sparkSession: SparkSession,
                       fullyQualifiedTableName: String,
                       parameters: Map[String, String] = Map()
                      ): HiveAcidTable = {

    val hiveAcidMetadata: HiveAcidMetadata =
      HiveAcidMetadata.fromSparkSession(sparkSession, fullyQualifiedTableName)
    new HiveAcidTable(sparkSession, hiveAcidMetadata, parameters)
  }
}

/**
  * Wrapper over [[HiveAcidTxn]] which ensures running operations within transaction boundary
  *
  * This wrapper can be used just once for running an operation. That operation is not allowed to recursively call this again
  * @param sparkSession
  */
private class HiveTxnWrapper(sparkSession: SparkSession) extends Logging {

  private var isLocalTxn: Boolean = _
  private var curTxn: HiveAcidTxn = _
  private object TxnWrapperState extends Enumeration {
    type state = Value
    val INIT, OPEN, CLOSE = Value
  }
  private var currentState: TxnWrapperState.state = TxnWrapperState.INIT

  private def isValidStateTransition(newState: TxnWrapperState.state): Boolean = {
    if (currentState == newState) {
      true
    } else {
      currentState match {
        case TxnWrapperState.INIT => newState == TxnWrapperState.OPEN
        case TxnWrapperState.OPEN => newState == TxnWrapperState.CLOSE
        case TxnWrapperState.CLOSE => newState == TxnWrapperState.CLOSE
      }
    }
  }

  private def transitionTo(newState: TxnWrapperState.state): Unit = {
    currentState match {
      case TxnWrapperState.INIT if newState == TxnWrapperState.OPEN =>
        currentState = TxnWrapperState.OPEN
      case TxnWrapperState.OPEN if newState == TxnWrapperState.CLOSE =>
        currentState = TxnWrapperState.CLOSE
      case _ if currentState != newState =>
        throw new IllegalArgumentException(s"Transition from $currentState to $newState is not allowed")
    }
  }

  // Start local transaction if not passed.
  private def getOrCreateTxn(): Unit = {
    if (!isValidStateTransition(TxnWrapperState.OPEN)) {
      throw new IllegalArgumentException(s"Transition from $currentState to ${TxnWrapperState.OPEN} is not allowed")
    }

    curTxn = HiveAcidTxn.currentTxn()
    curTxn match {
      case null =>
        // create local txn
        logInfo("Creating new transaction")
        curTxn = HiveAcidTxn.createTransaction(sparkSession)
        curTxn.begin()
        isLocalTxn = true
        logInfo(s"Transaction created: ${curTxn.txnId}")
      case txn =>
        logInfo(s"Using existing transaction:  ${curTxn.txnId}")
    }
    transitionTo(TxnWrapperState.OPEN)
  }

  // End and reset transaction and snapshot
  // if locally started
  private def unsetOrEndTxn(abort: Boolean = false): Unit = {
    if (!isValidStateTransition(TxnWrapperState.CLOSE)) {
      throw new IllegalArgumentException(s"Transition from $currentState to ${TxnWrapperState.CLOSE} is not allowed")
    }
    if (!isLocalTxn) {
      logInfo(s"Not ending the transaction ${curTxn.txnId} as it's not Local")
      return
    }
    logInfo(s"Ending Transaction ${curTxn.txnId} with abort flag: $abort")
    curTxn.end(abort)
    curTxn = null
    isLocalTxn = false
  }

  /**
    * Executes function `f` under transaction protection.
    * Following is the lifecycle of the Transaction here:
    *
    *  1. Create Transaction
    *  2. While creating valid transaction, also store the valid Transactions (`validTxns`) at that point.
    *  3. Acquire Locks
    *  4. `validTxns` could have changed at this point due to
    *      transactions getting committed between Step 2 and 3 above.
    *      Abort the transaction if it happened and Retry from step 1.
    *  5. Execute `f`
    *  6. End the transaction
    *
    *  Note, if transaction was already existing for this thread, it will be respected.
    * @param f Code Block to be executed in transaction
    */
  def inTxn(f: HiveAcidTxn => Unit): Unit = synchronized {

    // Pulled from thin air
    val maxInvalidTxnRetry = 2

    def inTxnRetry(retryRemaining: Int, f: HiveAcidTxn => Unit): Boolean = {
      getOrCreateTxn()
      var abort = false
      var retry = false
      try {
        f.apply(curTxn)
      }
      catch {
        case tie: TransactionInvalidException =>
          abort = true
          if (isLocalTxn && retryRemaining > 0) {
            logError(s"Transaction ${curTxn.txnId} was aborted as " +
              s"it became invalid before the locks were acquired ... Retrying", tie)
            retry = true
          } else {
            logError(s"Transaction ${curTxn.txnId} was aborted " +
              s"as it became invalid before the locks were acquired", tie)
            throw tie
          }
        case e: Exception =>
          logError("Unable to execute in transactions due to: " + e.getMessage)
          abort = true
          throw e
      }
      finally {
        unsetOrEndTxn(abort)
      }
      retry
    }
    var retryRemaining = maxInvalidTxnRetry - 1
    while (inTxnRetry(retryRemaining, f)) {
      retryRemaining = retryRemaining - 1
    }
  }
}

