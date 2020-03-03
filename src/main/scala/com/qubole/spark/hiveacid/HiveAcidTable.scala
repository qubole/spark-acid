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

import com.qubole.spark.hiveacid.reader.TableReader
import com.qubole.spark.hiveacid.writer.TableWriter
import com.qubole.spark.hiveacid.hive.HiveAcidMetadata
import com.qubole.spark.hiveacid.datasource.HiveAcidDataSource
import com.qubole.spark.hiveacid.rdd.EmptyRDD
import com.qubole.spark.hiveacid.transaction._
import org.apache.spark.annotation.InterfaceStability.Evolving
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.Column
import org.apache.spark.sql.SqlUtils
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.collection.JavaConverters._


/**
 * Represents a hive acid table and exposes API to perform operations on top of it
 * @param sparkSession - spark session object
 * @param hiveAcidMetadata - metadata object
 * @param parameters - additional parameters
 */
@Evolving
class HiveAcidTable(sparkSession: SparkSession,
                    hiveAcidMetadata: HiveAcidMetadata,
                    parameters: Map[String, String]
                   ) extends Logging {

  // Pulled from thin air
  private val maxInvalidTxnRetry = 5

  private var isLocalTxn: Boolean = false
  private var curTxn: HiveAcidTxn = _

  // Start local transaction if not passed.
  private def getOrCreateTxn(): Unit = {
    curTxn = HiveAcidTxn.currentTxn()
    curTxn match {
      case null =>
        // create local txn
        curTxn = HiveAcidTxn.createTransaction(sparkSession)
        curTxn.begin()
        isLocalTxn = true
      case txn =>
        logDebug(s"Existing Transactions $txn")
    }
  }

  // End and reset transaction and snapshot
  // if locally started
  private def unsetOrEndTxn(abort: Boolean = false): Unit = {
    if (! isLocalTxn) {
      return
    }
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
    * @param f
    */
  private def inTxn(f: => Unit): Unit = synchronized {
    def inTxnRetry(retryRemaining: Int): Boolean = {
      getOrCreateTxn()
      var abort = false
      var retry = false
      try {
        f
      }
      catch {
        case tie: TransactionInvalidException =>
          abort = true
          if (isLocalTxn && retryRemaining > 0) {
            logError(s"Transaction ${curTxn.txnId} was aborted as it became invalid before the locks were acquired ... Retrying", tie)
            retry = true
          } else {
            logError(s"Transaction ${curTxn.txnId} was aborted as it became invalid before the locks were acquired. Max retries reached", tie)
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
    while (inTxnRetry(retryRemaining)) {
      retryRemaining = retryRemaining - 1
    }
  }

  /**
    * Create dataframe to read based on hiveAcidTable and passed in filter.
    * @return Dataframe
    */
  private def readDF: DataFrame = {
    // Fetch row with rowID in it
    sparkSession.read.format(HiveAcidDataSource.NAME)
      .options(parameters ++
        Map("includeRowIds" -> "true", "table" -> hiveAcidMetadata.fullyQualifiedName))
      .load()
  }

  /**
    * Return df after after applying update clause and filter clause. This df is used to
    * update the table.
    * @param condition - condition string to identify rows which needs to be updated
    * @param newValues - Map of (column, value) to set
    */
  private def updateDF(condition: Option[String], newValues: Map[String, String]): DataFrame = {
    val conditionColumn = condition.map(functions.expr(_))
    val newValMap = newValues.mapValues(value => functions.expr(value))
    updateDFInternal(conditionColumn, newValMap)
  }


  /**
    * Return df after after applying update clause and filter clause. This df is used to
    * update the table.
    * @param condition - condition string to identify rows which needs to be updated
    * @param newValues - Map of (column, value) to set
    */
  private def updateDFInternal(condition: Option[Column], newValues: Map[String, Column]): DataFrame = {

    val (df: DataFrame, qualifiedPlan: LogicalPlan, resolvedDf: DataFrame) = getResolvedReadDF

    def toStrColumnMap(map: Map[String, Column]): Map[String, Column] = {
      map.toSeq.map { case (k, v) =>
        k.toLowerCase -> functions.expr(SqlUtils.resolveReferences(sparkSession, v.expr,
          qualifiedPlan).sql)}.toMap
    }

    val strColumnMap = toStrColumnMap(newValues)
    val updateColumns = strColumnMap.keys
    val resolver = sparkSession.sessionState.conf.resolver
    val resolvedOutput = resolvedDf.queryExecution.optimizedPlan.output.map(_.name)

    // Check if updateColumns are present
    val updateColumnNotFound = updateColumns.find(uc => !resolvedOutput.exists(o => resolver(o, uc)))
    updateColumnNotFound.map {
      u => throw HiveAcidErrors.updateSetColumnNotFound(u, resolvedOutput)
    }

    val updateExpressions: Seq[Expression] =
      resolvedDf.queryExecution.optimizedPlan.output.map {
        attr =>
          val updateColOpt = updateColumns.find(uc => resolver(uc, attr.name))
           updateColOpt match {
             case Some(updateCol) => strColumnMap(updateCol).expr
             case None => attr
          }
      }

    val newColumns = updateExpressions.zip(df.queryExecution.optimizedPlan.output).map {
      case (newExpr, origAttr) =>
        new Column(Alias(newExpr, origAttr.name)())
    }

    condition match {
      case Some(cond) => {
        val resolvedExpr = SqlUtils.resolveReferences(sparkSession,
          cond.expr,
          qualifiedPlan)
        resolvedDf.filter(resolvedExpr.sql).select(newColumns: _*)
      }
      case None => {
        resolvedDf.select(newColumns: _*)
      }
    }

  }

  private def getResolvedReadDF = {
    val df = readDF

    val plan = df.queryExecution.analyzed
    val qualifiedPlan = plan match {
      case p: LogicalRelation =>
        p.copy(output = p.output
          .map((x: AttributeReference) =>
            x.withQualifier(hiveAcidMetadata.fullyQualifiedName.split('.').toSeq))
        )
      case _ => plan
    }

    val newDf = SqlUtils.convertToDF(sparkSession, qualifiedPlan)
    (df, qualifiedPlan, newDf)
  }

  /**
    * Returns true if the table is an insert only table
    */
  def isInsertOnlyTable(): Boolean = {
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
    inTxn {
      val tableReader = new TableReader(sparkSession, curTxn, hiveAcidMetadata)
      res = tableReader.getRdd(requiredColumns, filters, readConf)
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
      val tableWriter = new TableWriter(sparkSession, curTxn, hiveAcidMetadata)
      tableWriter.process(HiveAcidOperation.INSERT_INTO, df)
      txnId = HiveAcidTxn.currentTxn().txnId
    }
    txnId
  }

  /**
    * Appends a given dataframe df into the hive acid table
    *
    * Note: This API is transactional in nature.
    * @param df - dataframe to insert
    */
  def insertInto(df: DataFrame): Unit = inTxn {
    val tableWriter = new TableWriter(sparkSession, curTxn, hiveAcidMetadata)
    tableWriter.process(HiveAcidOperation.INSERT_INTO, df)
  }

  /**
    * Overwrites a given dataframe df onto the hive acid table
    *
    * Note: This API is transactional in nature.
    * @param df - dataframe to insert
    */
  def insertOverwrite(df: DataFrame): Unit = inTxn {
    val tableWriter = new TableWriter(sparkSession, curTxn, hiveAcidMetadata)
    tableWriter.process(HiveAcidOperation.INSERT_OVERWRITE, df)
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
  def delete(condition: Column): Unit = {
    checkForSupport(HiveAcidOperation.DELETE)
    inTxn {
      val (_, qualifiedPlan: LogicalPlan, resolvedDf: DataFrame) = getResolvedReadDF
      val resolvedExpr = SqlUtils.resolveReferences(sparkSession,
        condition.expr,
        qualifiedPlan)
      val tableWriter = new TableWriter(sparkSession, curTxn, hiveAcidMetadata)
      tableWriter.process(HiveAcidOperation.DELETE, resolvedDf.filter(resolvedExpr.sql))
    }
  }

  /**
    * Update rows in the hive acid table based on condition and newValues
    *
    * Note: This API is transactional in nature.
    * @param condition - condition string to identify rows which needs to be updated
    * @param newValues - Map of (column, value) to set
    */
  @Evolving
  def update(condition: Option[String], newValues: Map[String, String]): Unit = {
    checkForSupport(HiveAcidOperation.UPDATE)
    inTxn {
      val updateDf = updateDF(condition, newValues)
      val tableWriter = new TableWriter(sparkSession, curTxn, hiveAcidMetadata)
      tableWriter.process(HiveAcidOperation.UPDATE, updateDf)
    }
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
  def update(condition: Option[Column], newValues: java.util.Map[String, Column]): Unit = {
    checkForSupport(HiveAcidOperation.UPDATE)
    inTxn {
      val updateDf = updateDFInternal(condition, newValues.asScala.toMap)
      val tableWriter = new TableWriter(sparkSession, curTxn, hiveAcidMetadata)
      tableWriter.process(HiveAcidOperation.UPDATE, updateDf)
    }
  }

  def isFullAcidTable(): Boolean = {
    hiveAcidMetadata.isFullAcidTable
  }

  def isBucketed(): Boolean = {
    hiveAcidMetadata.isBucketed
  }

  private def checkForSupport(operation: HiveAcidOperation.OperationType): Unit = {
    operation match {
      case HiveAcidOperation.UPDATE | HiveAcidOperation.UPDATE => {
        if (!this.isFullAcidTable() && !this.isInsertOnlyTable()) {
          throw HiveAcidErrors.tableNotAcidException(hiveAcidMetadata.fullyQualifiedName)
        }
        if (!this.isFullAcidTable() && this.isInsertOnlyTable()) {
          throw HiveAcidErrors.unsupportedOperationTypeInsertOnlyTable(operation.toString, hiveAcidMetadata.fullyQualifiedName)
        }
        if (this.isBucketed()) {
          throw HiveAcidErrors.unsupportedOperationTypeBucketedTable(operation.toString, hiveAcidMetadata.fullyQualifiedName)
        }
      }
    }
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
