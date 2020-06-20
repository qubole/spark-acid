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

import com.qubole.spark.hiveacid.datasource.HiveAcidRelation
import com.qubole.spark.hiveacid.{AnalysisException, HiveAcidErrors}
import org.apache.spark.sql.catalyst.analysis.{Star, UnresolvedAttribute, UnresolvedStar}
import org.apache.spark.sql.{SparkSession, SqlUtils}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, ExtractValue, GetStructField}
import org.apache.spark.sql.catalyst.parser.plans.logical.MergePlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

case class MergeCondition (expression: Expression)

sealed trait MergeWhenClause {
  def validate(): Unit
  def resolve(sparkSession: SparkSession, mergePlan: MergePlan): MergeWhenClause
}

object MergeWhenClause {

  val matchClauseConditionError: String = "If two WHEN Clause is specified in one " +
    "MERGE query, first one should have matching condition specified"

  val atleastOneClauseError: String = "At least one WHEN Clause required"

  val justOneClausePerTypeError: String = "More than one clause of INSERT," +
    " DELETE and UPDATE is not allowed in one MERGE query"

  def validate(clauses: Seq[MergeWhenClause]): Unit = {
    val matchedClause = clauses.collect {
      case c: MergeWhenUpdateClause => c
      case c: MergeWhenDelete => c
    }
    val updateClause = matchedClause.collect { case c: MergeWhenUpdateClause => c }
    val deleteClauses = matchedClause.collect { case c: MergeWhenDelete => c }
    val insertClauses = clauses.collect { case c: MergeWhenNotInsert => c }

    if (clauses.isEmpty) {
      throw HiveAcidErrors.mergeValidationError(atleastOneClauseError)
    }

    if (updateClause.size > 1 || deleteClauses.size > 1 || insertClauses.size > 1) {
      throw HiveAcidErrors.mergeValidationError(justOneClausePerTypeError)
    }

    if (matchedClause.size == 2) {
      val valid = matchedClause.head match {
        case MergeWhenUpdateClause(matchCondition, _, _) => matchCondition.isDefined
        case MergeWhenDelete(matchCondition) => matchCondition.isDefined
        case _ => throw new IllegalArgumentException("matched clause cannot have" +
          " operation except UPDATE and DELETE")
      }
      if (!valid) {
        throw HiveAcidErrors.mergeValidationError(matchClauseConditionError)
      }
    }
    clauses.foreach(_.validate())
  }

  def resolve(sparkSession: SparkSession, mergePlan: MergePlan,
              clauses: Seq[MergeWhenClause]): Seq[MergeWhenClause] = {
    clauses.map(_.resolve(sparkSession, mergePlan))
  }
}

case class MergeWhenUpdateClause(matchCondition: Option[Expression], setExpression: Map[String, Expression],
                                 isStar: Boolean)
  extends MergeWhenClause {

  def validate(): Unit = {
    if (setExpression.isEmpty && !isStar) {
      throw new IllegalArgumentException("UPDATE Clause in MERGE should have one or more SET Values")
    }
  }

  override def resolve(sparkSession: SparkSession, mergePlan: MergePlan): MergeWhenClause = {
    val resolveCondition = matchCondition.map(SqlUtils.
      resolveReferences(sparkSession, _, mergePlan.children, failIfUnresolved = true, Some("mergeUpdateClause")))
    val resolver = sparkSession.sessionState.conf.resolver
    val resolvedSetExpression: Map[String, Expression] = if (isStar) {
      // Represents UPDATE *. This is denoted by sequence of `targetColumnNames = sourceColumnName`
      // which are expected to be present in source too.
      // Hence, they will be resolved by sourceTable.
      var resolvedSetExpression: Map[String, Expression] = Map()
      mergePlan.targetPlan.output.map(_.name).foreach { colName =>
        val resolvedName = SqlUtils.resolveReferences(sparkSession, UnresolvedAttribute.quotedString(s"`$colName`"),
          mergePlan.sourcePlan, failIfUnresolved = true, Some("mergeUpdateClause"))
        resolvedSetExpression = resolvedSetExpression ++ Map(colName -> resolvedName)
      }
      resolvedSetExpression
    } else {
      setExpression.toList.map {
        entry => {
          val col = entry._1
          val setExpr = entry._2
          val matchingTargetCol = mergePlan.targetPlan.output.find(attr => resolver.apply(col, attr.name))
          // In SET both `col = ...` and `alias.col = ...` needs to be supported
          val resolvedColName = matchingTargetCol match {
            case Some(x) => x.name
            case None => {
              val resolvedCol = SqlUtils.resolveReferences(sparkSession,
                UnresolvedAttribute(col),
                mergePlan.targetPlan,
                true,
                Some("mergeUpdateClause"))
              extractColumnName(resolvedCol)
            }
          }
          val resolvedSetExpr =  SqlUtils.resolveReferences(sparkSession, setExpr,
            mergePlan.children, failIfUnresolved = true, Some("mergeUpdateClause"))
          (resolvedColName, resolvedSetExpr)
        }
      }.toMap
    }

    // Validate that resolvedSetExpression is not updating the partitioned column
    mergePlan.targetPlan.collectLeaves().head match {
      case LogicalRelation(hiveAcidRelation: HiveAcidRelation, _, _ , _) => if (hiveAcidRelation.getHiveAcidTable().isPartitioned) {
        val hiveAcidMetadata = hiveAcidRelation.getHiveAcidTable().hiveAcidMetadata
        val partitionCols = hiveAcidMetadata.partitionSchema.fieldNames
        val partitionColUpdated = resolvedSetExpression.keys.find(setCol =>
          partitionCols.find(resolver.apply(_, setCol)).nonEmpty)
        if (partitionColUpdated.nonEmpty) {
          throw HiveAcidErrors.updateOnPartition(partitionColUpdated.toSeq, hiveAcidMetadata.fullyQualifiedName)
        }
      }
      case _ =>
    }

    MergeWhenUpdateClause(resolveCondition, resolvedSetExpression, isStar)
  }

  def extractColumnName(expr: Expression): String = expr match {
    case attr: AttributeReference => attr.name

    case Alias(c, _) => extractColumnName(c)

    case _: GetStructField | _: ExtractValue => throw new AnalysisException("Updating nested fields is not supported.")

    case other =>
      throw new AnalysisException(s"Found unsupported expression '$other' while parsing target column name parts")
  }
}

case class MergeWhenDelete(matchCondition: Option[Expression])
  extends MergeWhenClause {

  def validate(): Unit = {}

  override def resolve(sparkSession: SparkSession, mergePlan: MergePlan): MergeWhenClause = {
    val resolveCondition = matchCondition.map(SqlUtils.
      resolveReferences(sparkSession, _, mergePlan.children, failIfUnresolved = true, Some("mergeDeleteClause")))
    MergeWhenDelete(resolveCondition)
  }
}

case class MergeWhenNotInsert(condition: Option[Expression], insertValues: Seq[Expression])
  extends MergeWhenClause {

  def validate(): Unit = {
    if (insertValues.isEmpty) {
      throw new IllegalArgumentException("Insert clause should have one or more expression." +
        " Even * is allowed")
    }
  }

  override def resolve(sparkSession: SparkSession, mergePlan: MergePlan): MergeWhenClause = {
    val resolveCondition = condition.map(SqlUtils.
      resolveReferences(sparkSession, _, mergePlan.sourcePlan, failIfUnresolved = true))
    val resolvedStarValues = if (insertValues.exists(_.isInstanceOf[Star])) {
      // Represents INSERT *. This is denoted by sequence of sourceTable which are expected to be present in
      // target too. Hence, they will be resolved by sourceTable
      mergePlan.sourcePlan.output.map { attr =>
        val resolvedCol =
        SqlUtils.resolveReferences(sparkSession, UnresolvedAttribute.quotedString(s"`${attr.name}`"),
          mergePlan.targetPlan, failIfUnresolved = false, None)
        if (!resolvedCol.resolved) {
          throw HiveAcidErrors.mergeResolutionError(s"mergeInsertClause using `*` " +
            s"failed to resolve: column `${attr.name}` is " +
            s"not present in target columns. `*` denotes sequence of " +
            s"source attributes expected to " +
            s"be present in target too")
        }
        attr
      }
    } else {
      Seq[Expression]()
    }
    val resolvedInsertValues: Seq[Expression] = insertValues.flatMap {
      case UnresolvedStar(_) => resolvedStarValues
      case expr: Expression => Seq(SqlUtils.resolveReferences(sparkSession, expr,
        mergePlan.sourcePlan, failIfUnresolved = true, Some("mergeInsertClause")))
    }

    if (resolvedInsertValues.size != mergePlan.targetPlan.output.size) {
      throw HiveAcidErrors.mergeResolutionError(s"Insert column length: ${resolvedInsertValues.size} " +
        s"mismatches with target column length: ${mergePlan.targetPlan.output.size}")
    }

    MergeWhenNotInsert(resolveCondition, resolvedInsertValues)
  }
}
