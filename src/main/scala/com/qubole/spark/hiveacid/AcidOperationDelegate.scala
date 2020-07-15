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

import com.qubole.spark.hiveacid.datasource.HiveAcidDataSource
import com.qubole.spark.hiveacid.hive.HiveAcidMetadata
import com.qubole.spark.hiveacid.merge.{MergeImpl, MergeWhenClause, MergeWhenNotInsert}
import com.qubole.spark.hiveacid.reader.TableReader
import com.qubole.spark.hiveacid.transaction._
import com.qubole.spark.hiveacid.writer.TableWriter
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.apache.spark.sql.catalyst.parser.plans.logical.MergePlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{Column, DataFrame, SparkSession, SqlUtils, _}

import scala.collection.JavaConverters._



/**
  * Delegate to perform ACID operations: UPDATE/DELETE/INSERT/READ
  * These APIs are not protected under Transactions, however most of them may need
  * transactions to be already open.
  */
trait AcidOperationDelegate {
  /**
    * Return an RDD on top of Hive ACID table
    *
    * @param requiredColumns - columns needed
    * @param filters - filters that can be pushed down to file format
    * @param readConf - read conf
    * @param curTxn - current Transaction under which ead will be done
    * @return
    */
  def getRdd(requiredColumns: Array[String],
             filters: Array[Filter],
             readConf: SparkAcidConf,
             curTxn: HiveAcidTxn): RDD[Row]

  /**
    * Used by streaming query to add a datframe to hive acid table.
    * @param df - dataframe to insert
    * @param curTxn - Transaction under which the operation is being done
    */
  def addBatch(df: DataFrame, curTxn: HiveAcidTxn): Unit

  /**
    * Appends a given dataframe df into the hive acid table
    *
    * Note: This API is transactional in nature.
    * @param df - dataframe to insert
    * @param curTxn - current transaction underwhich INSERT operation needs to run.
    * @param statementId Optional. In a same transaction, multiple statements like INSERT/UPDATE/DELETE
    *                    (like in case of MERGE) can be issued.
    *                    [[statementId]] has to be different for them to ensure delta collision
    *                    is avoided for them during writes.
    */
  def insertInto(df: DataFrame, curTxn: HiveAcidTxn, statementId: Option[Int] = None): Unit

  /**
    * Overwrites a given dataframe df onto the hive acid table
    *
    * Note: This API is transactional in nature.
    * @param df - dataframe to insert
    * @param curTxn - current transaction under which insertOverWrite need to be run
    * @param statementId Optional. In a same transaction, multiple statements like INSERT/UPDATE/DELETE
    *                    (like in case of MERGE) can be issued.
    *                    [[statementId]] has to be different for them to ensure delta collision
    *                    is avoided for them during writes.
    */
  def insertOverwrite(df: DataFrame, curTxn: HiveAcidTxn, statementId: Option[Int] = None): Unit

  /**
    * Update rows in the hive acid table based on condition and newValues
    *
    * Note: This API is transactional in nature.
    * @param condition - condition string to identify rows which needs to be updated
    * @param newValues - Map of (column, value) to set
    */
  def update(condition: Option[String], newValues: Map[String, String], curTxn: HiveAcidTxn): Unit
  def update(condition: Option[Column], newValues: java.util.Map[String, Column], curTxn: HiveAcidTxn): Unit

  /**
    * Merge from sourceDf to the current Table
    *
    * @param sourceDf
    * @param hiveAcidMetadata
    * @param mergeExpression
    * @param matchedClause
    * @param notMatched
    * @param sourceAlias
    * @param targetAlias
    */
  def merge(sourceDf: DataFrame,
            hiveAcidMetadata: HiveAcidMetadata,
            mergeExpression: Expression,
            matchedClause: Seq[MergeWhenClause],
            notMatched: Option[MergeWhenNotInsert],
            sourceAlias: Option[AliasIdentifier],
            targetAlias: Option[AliasIdentifier],
            txn: HiveAcidTxn): Unit

  /**
    * Delete rows from the table based on `condtional` expression.
    *
    * Note: This API is transactional in nature.
    * @param condition - Boolean SQL Expression filtering rows to be deleted
    * @param curTxn - Transaction under which DELETE will be performed
    */
  def delete(condition: Column, curTxn: HiveAcidTxn): Unit

  /**
    * Delete dataframe of rows specified by MERGE Delete statement
    *
    * @param mergeDeleteDf dataframe of rows specified by MERGE Delete statement
    * @param curTxn Transaction under which this operation will be performed
    * @param statementId Specify Statement Id, for multi operation command like Merge statement Id
    *                    need to be unique for every operation
    */
  def mergeDelete(mergeDeleteDf: DataFrame, curTxn: HiveAcidTxn, statementId: Option[Int] = None): Unit

  /**
    * Update dataframe with updated rows specified by MERGE update statement
    *
    * @param mergeUpdateDf - Dataframe with updated rows specified by MERGE update statement
    * @param curTxn - Transaction under which this operation will be performed
    * @param statementId - Specify Statement Id, for multi operation command like Merge statement Id
    *                    need to be unique for every operation
    */
  def mergeUpdate(mergeUpdateDf : DataFrame, curTxn: HiveAcidTxn, statementId: Option[Int] = None): Unit
}

/**
  * [[HiveAcidTable]] uses Delegate pattern to delegate it's API to this object
  * @param sparkSession - spark session object
  * @param hiveAcidMetadata - metadata object
  * @param parameters - additional parameters
  */
class HiveAcidOperationDelegate(val sparkSession: SparkSession,
                                val hiveAcidMetadata: HiveAcidMetadata,
                                val parameters: Map[String, String]
                   ) extends AcidOperationDelegate with Logging {

  private val sparkAcidConfig = SparkAcidConf(sparkSession, parameters)

  /**
    * Create dataframe to read based on hiveAcidTable and passed in filter.
    * @return Dataframe
    */
  private def readDF(withRowId: Boolean): DataFrame = {
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
    * Return df after after applying update clause and filter clause. This df is used to
    * update the table.
    * @param condition - condition string to identify rows which needs to be updated
    * @param newValues - Map of (column, value) to set
    */
  private def updateDF(condition: Option[String], newValues: Map[String, String]): DataFrame = {
    val conditionColumn = condition.map(functions.expr)
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

    val df = readDF(true)
    val (qualifiedPlan: LogicalPlan, resolvedDf: DataFrame) =
      SqlUtils.getDFQualified(sparkSession, readDF(true), hiveAcidMetadata.fullyQualifiedName)

    def toStrColumnMap(map: Map[String, Column]): Map[String, Column] = {
      map.toSeq.map { case (k, v) =>
        k.toLowerCase -> functions.expr(SqlUtils.resolveReferences(sparkSession, v.expr,
          qualifiedPlan, failIfUnresolved = false).sql)}.toMap
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

    val outputColumns = updateExpressions.zip(df.queryExecution.optimizedPlan.output).map {
      case (newExpr, origAttr) =>
        new Column(Alias(newExpr, origAttr.name)())
    }

    condition match {
      case Some(cond) =>
        val resolvedExpr = SqlUtils.resolveReferences(sparkSession,
          cond.expr,
          qualifiedPlan, failIfUnresolved = false)
        resolvedDf.filter(resolvedExpr.sql).select(outputColumns: _*)
      case None =>
        resolvedDf.select(outputColumns: _*)
    }

  }

  /**
    * Return an RDD on top of Hive ACID table
    *
    * @param requiredColumns - columns needed
    * @param filters - filters that can be pushed down to file format
    * @param readConf - read conf
    * @return
    */
  def getRdd(requiredColumns: Array[String],
             filters: Array[Filter],
             readConf: SparkAcidConf,
             curTxn: HiveAcidTxn): RDD[Row] = {
    val tableReader = new TableReader(sparkSession, curTxn, hiveAcidMetadata)
    tableReader.getRdd(requiredColumns, filters, readConf)
  }

  /**
    * Used by streaming query to add a datframe to hive acid table.
    * @param df - dataframe to insert
    * @param curTxn - Transaction under which the operation is being done
    */
  def addBatch(df: DataFrame, curTxn: HiveAcidTxn): Unit = {
    val tableWriter = new TableWriter(sparkSession, curTxn, hiveAcidMetadata, sparkAcidConfig)
    tableWriter.process(HiveAcidOperation.INSERT_INTO, df)
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
  def insertInto(df: DataFrame, curTxn: HiveAcidTxn, statementId: Option[Int] = None): Unit = {
    val tableWriter = new TableWriter(sparkSession, curTxn, hiveAcidMetadata,
      sparkAcidConfig, statementId)
    tableWriter.process(HiveAcidOperation.INSERT_INTO, df)
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
  def insertOverwrite(df: DataFrame, curTxn: HiveAcidTxn, statementId: Option[Int] = None): Unit = {
    val tableWriter = new TableWriter(sparkSession, curTxn, hiveAcidMetadata,
      sparkAcidConfig, statementId)
    tableWriter.process(HiveAcidOperation.INSERT_OVERWRITE, df)
  }

  /**
    * Delete rows from the table based on `condtional` expression.
    *
    * Note: This API is transactional in nature.
    * @param condition - Boolean SQL Expression filtering rows to be deleted
    */
  def delete(condition: Column, curTxn: HiveAcidTxn): Unit = {
    checkForSupport(HiveAcidOperation.DELETE)
    val (qualifiedPlan: LogicalPlan, resolvedDf: DataFrame) =
      SqlUtils.getDFQualified(sparkSession, readDF(true), hiveAcidMetadata.fullyQualifiedName)
    val resolvedExpr = SqlUtils.resolveReferences(sparkSession,
      condition.expr,
      qualifiedPlan, failIfUnresolved = false)
    val tableWriter = new TableWriter(sparkSession, curTxn, hiveAcidMetadata, sparkAcidConfig)
    tableWriter.process(HiveAcidOperation.DELETE, resolvedDf.filter(resolvedExpr.sql))
  }

  /**
    * NOT TO BE USED EXTERNALLY
    * Not protected by transactionality, it is assumed curTxn is already
    * set before calling this
    * @param deleteDf DataFrame to be used to update is supposed to have
    *                 same schema as tableSchemaWithRowId
    * @param statementId Optional. In a same transaction, multiple statements like INSERT/UPDATE/DELETE
    *                    (like in case of MERGE) can be issued.
    *                    [[statementId]] has to be different for them to ensure delta collision
    *                    is avoided for them during writes.
    */
  def mergeDelete(deleteDf: DataFrame, curTxn: HiveAcidTxn, statementId: Option[Int] = None): Unit = {
    if (curTxn == null) {
      throw new IllegalStateException("Transaction not set before invoking update on dataframe")
    }
    if (deleteDf.schema != hiveAcidMetadata.tableSchemaWithRowId) {
      throw new UnsupportedOperationException("Delete Dataframe doesn't have expected schema. " +
        "Provided: " + deleteDf.schema.mkString(",") +
        "  Expected: " + hiveAcidMetadata.tableSchemaWithRowId.mkString(","))
    }
    val tableWriter = new TableWriter(sparkSession, curTxn, hiveAcidMetadata,
      sparkAcidConfig, statementId)
    tableWriter.process(HiveAcidOperation.DELETE, deleteDf)
  }

  /**
    * Update rows in the hive acid table based on condition and newValues
    *
    * Note: This API is transactional in nature.
    * @param condition - condition string to identify rows which needs to be updated
    * @param newValues - Map of (column, value) to set
    */
  def update(condition: Option[String], newValues: Map[String, String], curTxn: HiveAcidTxn): Unit = {
    checkForSupport(HiveAcidOperation.UPDATE)
    val partCols = hiveAcidMetadata.partitionSchema.fieldNames.map(_.toLowerCase)
    val partUpdateCols = newValues.keys.map(_.toLowerCase).filter(partCols.contains(_))
    if (!partUpdateCols.isEmpty) {
      throw HiveAcidErrors.updateOnPartition(partUpdateCols.toSeq, hiveAcidMetadata.fullyQualifiedName)
    }
    val updateDf = updateDF(condition, newValues)
    val tableWriter = new TableWriter(sparkSession, curTxn, hiveAcidMetadata, sparkAcidConfig)
    tableWriter.process(HiveAcidOperation.UPDATE, updateDf)
  }

  /**
    * Update rows in the hive acid table based on condition and newValues
    *
    * Note: This API is transactional in nature.
    * @param condition - Optional condition string to identify rows which needs to be updated,
    *                  if not specified then it means complete table.
    * @param newValues - Map of (column, value) to set
    */
  def update(condition: Option[Column], newValues: java.util.Map[String, Column], curTxn: HiveAcidTxn): Unit = {
    checkForSupport(HiveAcidOperation.UPDATE)
    val partCols = hiveAcidMetadata.partitionSchema.fieldNames.map(_.toLowerCase)
    val partUpdateCols = newValues.keySet().asScala.map(_.toLowerCase).filter(partCols.contains(_))
    if (!partUpdateCols.isEmpty) {
      throw HiveAcidErrors.updateOnPartition(partUpdateCols.toSeq,
        hiveAcidMetadata.fullyQualifiedName)
    }
    val updateDf = updateDFInternal(condition, newValues.asScala.toMap)
    val tableWriter = new TableWriter(sparkSession, curTxn, hiveAcidMetadata, sparkAcidConfig)
    tableWriter.process(HiveAcidOperation.UPDATE, updateDf)
  }

  /**
    * Uodated Df is written to the table
    * @param updateDf DataFrame to be used to update is supposed to have same schema as tableSchemaWithRowId
    * @param statementId Optional. In a same transaction, multiple statements like INSERT/UPDATE/DELETE
    *                    (like in case of MERGE) can be issued.
    *                    [[statementId]] has to be different for them to ensure delta collision
    *                    is avoided for them during writes.
    */
  def mergeUpdate(updateDf : DataFrame, curTxn: HiveAcidTxn, statementId: Option[Int] = None): Unit = {
    if (curTxn == null) {
      throw new IllegalStateException("Transaction not set before invoking update on dataframe")
    }
    if (updateDf.schema != hiveAcidMetadata.tableSchemaWithRowId) {
      throw new UnsupportedOperationException("Update Dataframe doesn't have expected schema. " +
        "Provided: " + updateDf.schema.mkString(",") +
        "  Expected: " + hiveAcidMetadata.tableSchemaWithRowId.mkString(","))
    }
    val tableWriter = new TableWriter(sparkSession, curTxn, hiveAcidMetadata,
      sparkAcidConfig, statementId)
    tableWriter.process(HiveAcidOperation.UPDATE, updateDf)
  }

  def merge(sourceDf: DataFrame,
            hiveAcidMetadata: HiveAcidMetadata,
            mergeExpression: Expression,
            matchedClause: Seq[MergeWhenClause],
            notMatched: Option[MergeWhenNotInsert],
            sourceAlias: Option[AliasIdentifier],
            targetAlias: Option[AliasIdentifier],
            curTxn: HiveAcidTxn): Unit = {
    def getPlan(df: DataFrame, alias: Option[AliasIdentifier], isTarget: Boolean) = {
      val finalDf = if (isTarget) {
        // Remove rowId from target as it's not visible to users
        df.drop(HiveAcidMetadata.rowIdCol)
      } else {
        df
      }
      alias match {
        case Some(alias) => SubqueryAlias(alias, finalDf.queryExecution.analyzed)
        case _ => finalDf.queryExecution.analyzed
      }
    }

    checkForSupport(HiveAcidOperation.MERGE)
    MergeWhenClause.validate(matchedClause ++ notMatched)
    // Take care of aliases for resolution.
    // readDF will be used for the actual merge operation. So resolution of merge clauses should happen on `readDF().queryExecution.analyzed`
    // As `readDF` has different attribute Ids assigned than `targetTable` we have to replace it
    // and make sure we use same readDF for merge operation.
    val (_, targetDf) = SqlUtils.getDFQualified(sparkSession, readDF(true),
      hiveAcidMetadata.fullyQualifiedName)
    val sourcePlan = getPlan(sourceDf, sourceAlias, isTarget = false)
    val targetPlan = getPlan(targetDf, targetAlias, isTarget = true)
    val mergeImpl = new MergeImpl(sparkSession, hiveAcidMetadata, this, sourceDf, targetDf,
      new MergePlan(sourcePlan, targetPlan, mergeExpression, matchedClause, notMatched), curTxn)
    mergeImpl.run()
  }

  private def checkForSupport(operation: HiveAcidOperation.OperationType): Unit = {

    def isFullAcidTable: Boolean = {
      hiveAcidMetadata.isFullAcidTable
    }

    def isBucketed: Boolean = {
      hiveAcidMetadata.isBucketed
    }

    def isInsertOnlyTable: Boolean = {
      hiveAcidMetadata.isInsertOnlyTable
    }

    operation match {
      case HiveAcidOperation.UPDATE | HiveAcidOperation.DELETE | HiveAcidOperation.MERGE =>
        if (!isFullAcidTable && !isInsertOnlyTable) {
          throw HiveAcidErrors.tableNotAcidException(hiveAcidMetadata.fullyQualifiedName)
        }
        if (!isFullAcidTable && isInsertOnlyTable) {
          throw HiveAcidErrors.unsupportedOperationTypeInsertOnlyTable(operation.toString,
            hiveAcidMetadata.fullyQualifiedName)
        }
        if (isBucketed) {
          throw HiveAcidErrors.unsupportedOperationTypeBucketedTable(operation.toString,
            hiveAcidMetadata.fullyQualifiedName)
        }
    }
  }
}

