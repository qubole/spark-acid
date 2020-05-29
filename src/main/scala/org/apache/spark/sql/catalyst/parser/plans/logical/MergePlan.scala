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

package org.apache.spark.sql.catalyst.parser.plans.logical

import com.qubole.spark.hiveacid.merge.{MergeWhenClause}
import org.apache.spark.sql.{SparkSession, SqlUtils}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}

case class MergePlan(sourcePlan: LogicalPlan,
                     targetPlan: LogicalPlan,
                     condition: Expression,
                     matched: Seq[MergeWhenClause],
                     notMatched: Option[MergeWhenClause]) extends Command {
  override def children: Seq[LogicalPlan] = Seq(sourcePlan, targetPlan)
  override def output: Seq[Attribute] = Seq.empty
}

object MergePlan {
  def resolve(sparkSession: SparkSession, mergePlan: MergePlan): MergePlan = {
    MergeWhenClause.validate(mergePlan.matched ++ mergePlan.notMatched)
    val resolvedCondition = SqlUtils.resolveReferences(sparkSession, mergePlan.condition,
      mergePlan.children, true, None)
    val resolvedMatched = MergeWhenClause.resolve(sparkSession, mergePlan, mergePlan.matched)
    val resolvedNotMatched = mergePlan.notMatched.map {
      x => x.resolve(sparkSession, mergePlan)
    }

    MergePlan(mergePlan.sourcePlan,
      mergePlan.targetPlan,
      resolvedCondition,
      resolvedMatched,
      resolvedNotMatched)
  }
}