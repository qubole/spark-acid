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

package com.qubole.spark.datasources.hiveacid.sql.catalyst.plans.command

import com.qubole.spark.hiveacid.HiveAcidErrors
import com.qubole.spark.hiveacid.datasource.HiveAcidRelation
import com.qubole.spark.hiveacid.merge.{MergeCondition, MergeWhenClause, MergeWhenNotInsert}
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.{Row, SparkSession, SqlUtils}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation

case class MergeCommand(targetTable: LogicalPlan,
                        sourceTable: LogicalPlan,
                        matched: Seq[MergeWhenClause],
                        notMatched: Option[MergeWhenClause],
                        mergeCondition: MergeCondition,
                        sourceAlias: Option[AliasIdentifier],
                        targetAlias: Option[AliasIdentifier])
  extends RunnableCommand {

  override def children: Seq[LogicalPlan] = Seq(targetTable, sourceTable)
  override def output: Seq[Attribute] = Seq.empty
  override lazy val resolved: Boolean = childrenResolved
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val insertClause: Option[MergeWhenNotInsert] = notMatched match {
      case Some(i: MergeWhenNotInsert) => Some(i)
      case None => None
      case _ => throw HiveAcidErrors.mergeValidationError("WHEN NOT Clause has to be INSERT CLAUSE")
    }

    children.head match {
      case LogicalRelation(relation: HiveAcidRelation, _, _ , _) =>
        relation.merge(SqlUtils.logicalPlanToDataFrame(sparkSession, sourceTable),
          mergeCondition.expression, matched, insertClause, sourceAlias, targetAlias)
      case SubqueryAlias(_, LogicalRelation(relation: HiveAcidRelation, _, _, _)) =>
        relation.merge(SqlUtils.logicalPlanToDataFrame(sparkSession, sourceTable),
          mergeCondition.expression, matched, insertClause, sourceAlias, targetAlias)
      case _ => throw HiveAcidErrors.tableNotAcidException(targetTable.toString())
    }

    Seq.empty
  }
}