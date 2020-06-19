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

package com.qubole.spark.hiveacid.merge

import com.qubole.spark.hiveacid.hive.HiveAcidMetadata
import com.qubole.spark.hiveacid.transaction.HiveAcidTxn
import com.qubole.spark.hiveacid.{AcidOperationDelegate, HiveAcidErrors, HiveAcidOperation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, Literal, Not}
import org.apache.spark.sql.{Column, DataFrame, SparkSession, SqlUtils, functions}
import org.apache.spark.sql.catalyst.parser.plans.logical.MergePlan
import org.apache.spark.internal.Logging

/**
  * Implements Algorithm to do Merge
  * It does target right outer join source on the merge condition specified.
  * MergeJoin = target right outer join source
  *
  * Filter(MergeJoin, target.rowId != null) will provide the matched rows for UPDATE/DELETE
  * Filter(MergeJoin, target.rowId == null) will provide non-matched rows for INSERT
  *
  * DFs with the rows to UPDATE/DELETE/INSERT are created and corresponding operations
  * are performed on [[HiveAcidTable]]. Under same transactions, different statementIds
  * are assigned to each operation so that they don;t collide while writing delta files.
  *
  * Performance considerations:
  *
  * Special handling is done where only INSERT clause is provided.
  * In such case source left anti join target gives the rows to be
  * inserted instead of expensive right outer join.
  *
  * We donot want right outer join to happen for every operation.
  * So join dataframe is converted to RDD and back to Dataframe.
  * This ensures that transformations on the converted DataFrame
  * donot recompute the RDD i.e., join is executed just once.
  *
  * According to SQL standard we need to error when multiple source
  * rows match same target row. We use the same join done above for
  * other operations to figure that out instead of running more joins.
  *
  * @param sparkSession
  * @param hiveAcidMetadata
  * @param sourceDf
  * @param targetDf
  * @param mergePlan
  */
class MergeImpl(val sparkSession: SparkSession,
                val hiveAcidMetadata: HiveAcidMetadata,
                val operationDelegate: AcidOperationDelegate,
                val sourceDf: DataFrame,
                val targetDf: DataFrame,
                mergePlan: MergePlan,
                val txn: HiveAcidTxn)
  extends Logging {

  private val resolvedMergePlan = MergePlan.resolve(sparkSession, mergePlan)
  // Condition for finding matching rows from Merge Join: target right Outer Join source
  // target.rowIds != null
  var resolvedMatchedCond: Option[Expression] = None
  // Condition for finding matching rows from Merge Join: target right Outer Join source
  // target.rowIds == null
  var resolvedNonMatchedCond: Option[Expression] = None

  private def getNonMatchedOperation(joinedDf: DataFrame): Option[MergeDFOperation] = {
    if (resolvedNonMatchedCond.isEmpty) {
      throw new IllegalArgumentException("Invalid state during Merge execution: " +
        "nonMatchCondition should be resolved by now")
    }
    resolvedMergePlan.notMatched match {
      case Some(x: MergeWhenNotInsert) =>
        val targetOutputCols = targetDf.drop(HiveAcidMetadata.rowIdCol).queryExecution.analyzed.output
        val outputCols = targetOutputCols.zip(x.insertValues).map {
          case (attr, value) => new Column(Alias(value , attr.name)())
        }
        val insertDf = joinedDf.filter(new Column(resolvedNonMatchedCond.get))
          .filter(x.condition.map(new Column(_)).getOrElse(functions.lit(true)))
          .select(outputCols :_*)
        Some(MergeDFOperation(insertDf, HiveAcidOperation.INSERT_INTO))

      case None => None
      case _ => throw HiveAcidErrors.mergeValidationError(
        "non matched clause can only be Insert in MERGE")
    }
  }

  private def getMatchedDf(joinedDf: DataFrame, matchClause: MergeWhenClause,
                           targetColumnsWithRowId: Seq[Attribute]): MergeDFOperation = {
    if (resolvedMatchedCond.isEmpty) {
      throw new IllegalArgumentException("Invalid state during Merge execution: " +
        "matchCondition should be resolved by now")
    }
    val resolver = sparkSession.sessionState.conf.resolver
    matchClause match {
      case MergeWhenDelete(matchCondition)  =>
        val deleteDf = (matchCondition match {
          case Some(condition) => {
            joinedDf.filter(new Column(resolvedMatchedCond.get)).
              filter(new Column(condition))
          }
          case _ => joinedDf.filter(new Column(resolvedMatchedCond.get))
        }).select(targetColumnsWithRowId.map(new Column(_)): _*)
        MergeDFOperation(deleteDf, HiveAcidOperation.DELETE)

      case MergeWhenUpdateClause(matchCondition, setExpression: Map[String, Expression], _) =>
        val updateColumns = setExpression.keys
        val updateExpressions: Seq[Expression] =
          targetColumnsWithRowId.map {
            col =>
              val updateColOpt = updateColumns.find(uc => resolver(uc, col.name))
              updateColOpt match {
                case Some(updateCol) => setExpression(updateCol)
                case None => col
              }
          }
        val outputColumns = updateExpressions.zip(targetColumnsWithRowId).map {
          case (newExpr, origAttr: Attribute) =>
            new Column(Alias(newExpr, origAttr.name)())
        }
        val updateDf = (matchCondition match {
          case Some(matchCondition) => joinedDf.filter(new Column(resolvedMatchedCond.get)).
            filter(new Column(matchCondition))
          case None => joinedDf.filter(new Column(resolvedMatchedCond.get))
        }).select(outputColumns: _*)
        MergeDFOperation(updateDf, HiveAcidOperation.UPDATE)

      case _ => throw HiveAcidErrors.mergeValidationError("matched clause cannot have" +
          " operation except UPDATE and DELETE")
    }
  }

  def run(): Unit = {
    logInfo(s"MERGE operation being done on table ${hiveAcidMetadata.tableName}")
    if (resolvedMergePlan.matched.isEmpty) {
      // it should have been validated that at least one merge clause is present
      runForJustInsertClause(resolvedMergePlan)
    } else {
      val baseDf = targetDf.join(sourceDf,
        new Column(resolvedMergePlan.condition), "rightOuter")
      val joinedDf = SqlUtils.createDataFrameUsingAttributes(sparkSession, baseDf.rdd,
        baseDf.schema, baseDf.queryExecution.analyzed.output)
      val joinedPlan = joinedDf.queryExecution.analyzed
      val matchedCond = functions.col(HiveAcidMetadata.rowIdCol).isNotNull.expr
      resolvedMatchedCond = Some(SqlUtils.resolveReferences(sparkSession,
        matchedCond, joinedPlan, failIfUnresolved = false))
      val nonMatchedCond = functions.col(HiveAcidMetadata.rowIdCol).isNull.expr
      resolvedNonMatchedCond = Some(SqlUtils.resolveReferences(sparkSession,
        nonMatchedCond, joinedPlan, failIfUnresolved = false))
      val targetPlan = targetDf.queryExecution.analyzed
      val targetColumnsWithRowId = targetPlan.output

      val targetPartColsWithRowId = (Seq(HiveAcidMetadata.rowIdCol) ++
        hiveAcidMetadata.partitionSchema.fieldNames).map( col =>
        SqlUtils.resolveReferences(sparkSession, functions.col(col).expr,
          targetPlan, failIfUnresolved = true))

      // Get MergeOperations that needs to be executed in different statements
      val operationDfs: Seq[MergeDFOperation] =
        getMatchedOperations(joinedDf, targetColumnsWithRowId) ++ getNonMatchedOperation(joinedDf)


      if (operationDfs.nonEmpty) {
        // SQL Standard says to error out when multiple source columns match with 1 target column
        // So after right outer join, we can group by rowIds (which are unique for every partition)
        // to figure that out
        logInfo("Cardinality Check for Merge being Performed to check one " +
          "row of target matches with utmost one source row. Note this check requires Join and can take time.")
        val targetRowsWithMultipleMatch = joinedDf
          .filter(new Column(matchedCond))
          .groupBy(targetPartColsWithRowId.map(new Column(_)) :_*)
          .count()
          .filter("count > 1")
          .count()
        if (targetRowsWithMultipleMatch > 0) {
          HiveAcidErrors.mergeUnsupportedError(s"MERGE is not supported when multiple rows" +
            s" of source match same target row. " +
            s"$targetRowsWithMultipleMatch rows in target had multiple matches." +
            s" Please check MERGE match condition and try again")
        }

        runMergeOperations(operationDfs)
      }
    }
  }

  private def runMergeOperations(operationDfs: Seq[MergeDFOperation]): Unit = {
    def getStatementId(i: Int): Option[Int] = {
      if (operationDfs.size == 1) {
        None
      } else {
        Some(i)
      }
    }
    logInfo("MERGE requires right outer join between Target and Source.")
    operationDfs.view.zipWithIndex.foreach {
      case (op: MergeDFOperation, index: Int) => {
        op match {
          case MergeDFOperation(df, HiveAcidOperation.DELETE) =>
            logInfo(s"MERGE Clause ${index+1}: DELETE being executed")
            operationDelegate.mergeDelete(df, txn, getStatementId(index))
          case MergeDFOperation(df, HiveAcidOperation.UPDATE) =>
            logInfo(s"MERGE Clause ${index+1}: UPDATE being executed")
            operationDelegate.mergeUpdate(df, txn, getStatementId(index))
          case MergeDFOperation(df, HiveAcidOperation.INSERT_INTO) =>
            logInfo(s"MERGE Clause ${index+1}: INSERT being executed")
            operationDelegate.insertInto(df, txn, getStatementId(index))
          case MergeDFOperation(_, operationType) =>
            HiveAcidErrors.mergeValidationError(s"Operation type $operationType is not supported in MERGE")
        }
      }
      case _ => throw HiveAcidErrors.mergeUnsupportedError("Invalid MergeOperation while executing MERGE")
    }
    logInfo("All MERGE Clauses were executed")
  }

  private def getMatchedOperations(joinedDf: DataFrame,
                           targetColumnsWithRowId: Seq[Attribute]): Seq[MergeDFOperation] = {
    // Extract first Match Condition.
    // 1. If there are 2 match conditions and a row matches both condition,
    //    only the first clause will be executed
    // 2. If there are 2 match clauses, first one should have a condition.
    val firstMatchCondition = if (resolvedMergePlan.matched.isEmpty) {
      Literal(true)
    } else {
      resolvedMergePlan.matched.head match {
        case MergeWhenDelete(matchCondition) => matchCondition.getOrElse(Literal(true))
        case MergeWhenUpdateClause(matchCondition, _, _) => matchCondition.getOrElse(Literal(true))
        case _ => Literal(true)
      }
    }

    resolvedMergePlan.matched.size match {
      case 0 => Seq[MergeDFOperation]()
      case 1 =>
        val matchedDf = getMatchedDf(joinedDf, resolvedMergePlan.matched.head, targetColumnsWithRowId)
        Seq(matchedDf)
      case 2 => {
        val matchedDf1 =
          getMatchedDf(joinedDf, resolvedMergePlan.matched.head, targetColumnsWithRowId)
        // For second clause exclude the rows matched by the first condition
        val matchedDf2 =
          getMatchedDf(joinedDf.filter(new Column(Not(firstMatchCondition))),
            resolvedMergePlan.matched(1), targetColumnsWithRowId)
        Seq(matchedDf1, matchedDf2)
      }
      case _ => throw HiveAcidErrors.mergeValidationError(
        "Not more than 2 Match Clause can to be specified in MERGE")
    }
  }

  private def runForJustInsertClause(mergePlan: MergePlan): Unit = {
    val notMatched: MergeWhenNotInsert = mergePlan.notMatched match {
      case Some(x: MergeWhenNotInsert) => x
      case None => throw new IllegalArgumentException("Should contain one INSERT clause")
      case _ => throw  HiveAcidErrors.mergeValidationError("MERGE" +
        " WHEN NOT clause can only be INSERT Clause")
    }
    val targetOutputColNames = targetDf.drop(HiveAcidMetadata.rowIdCol).columns
    if (targetOutputColNames.length != notMatched.insertValues.length) {
      throw HiveAcidErrors.mergeValidationError("Insert values mismatch" +
        " with Target columns. Target table has " + targetOutputColNames.length +
        " columns, whereas insert specifies " + notMatched.insertValues.length + " values.")
    }
    val outputCols = targetOutputColNames.zip(notMatched.insertValues).map {
      case (col, value) => new Column(Alias(value , col)())
    }

    val resolvedCondition = notMatched.condition.getOrElse(functions.lit(true).expr)
    logInfo(s"MERGE Clause ${1}: INSERT being executed")
    logInfo(s"Note, only MERGE operation specified is INSERT hence, LeftAntiJoin " +
      s"between source and target will be performed")
    // As only notMatched Clauses are present, we can do left-anti join to find the rows to be inserted
    val insertDf = sourceDf.join(targetDf, new Column(mergePlan.condition), "leftanti")
      .filter(new Column(resolvedCondition)).select(outputCols :_*)
    operationDelegate.insertInto(insertDf, txn)
  }
  private case class MergeDFOperation(dataFrame: DataFrame, operationType: HiveAcidOperation.OperationType)
}


