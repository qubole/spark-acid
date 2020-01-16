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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.expressions.{Expression, Attribute}

object SqlUtils {
  def convertToDF(sparkSession: SparkSession, plan : LogicalPlan): DataFrame = {
    Dataset.ofRows(sparkSession, plan)
  }
  
  def resolveReferences(sparkSession: SparkSession,
                                   expr: Expression,
                                   planContaining: LogicalPlan): Expression = {
    val newPlan = FakeLogicalPlan(expr, Seq(planContaining))
    sparkSession.sessionState.analyzer.execute(newPlan) match {
      case FakeLogicalPlan(resolvedExpr: Expression, _) =>
        // Return even if it did not successfully resolve
        resolvedExpr
      case _ =>
        expr
      // This is unexpected
    }
  }
  def hasSparkStopped(sparkSession: SparkSession): Boolean = {
    sparkSession.sparkContext.stopped.get()
  }
}

case class FakeLogicalPlan(expr: Expression, children: Seq[LogicalPlan])
  extends LogicalPlan {
  override def output: Seq[Attribute] = children.foldLeft(Seq[Attribute]())((out, child) => out ++ child.output)
}
