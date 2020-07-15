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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.StructType

object SqlUtils {
  def convertToDF(sparkSession: SparkSession, plan : LogicalPlan): DataFrame = {
    Dataset.ofRows(sparkSession, plan)
  }

  def resolveReferences(sparkSession: SparkSession,
                        expr: Expression,
                        planContaining: LogicalPlan, failIfUnresolved: Boolean,
                        exprName: Option[String] = None): Expression = {
    resolveReferences(sparkSession, expr, Seq(planContaining), failIfUnresolved, exprName)
  }

  def resolveReferences(sparkSession: SparkSession,
                        expr: Expression,
                        planContaining: Seq[LogicalPlan],
                        failIfUnresolved: Boolean,
                        exprName: Option[String]): Expression = {
    val newPlan = FakeLogicalPlan(expr, planContaining)
    val resolvedExpr = sparkSession.sessionState.analyzer.execute(newPlan) match {
      case FakeLogicalPlan(resolvedExpr: Expression, _) =>
        // Return even if it did not successfully resolve
        resolvedExpr
      case _ =>
        expr
      // This is unexpected
    }
    if (failIfUnresolved) {
      resolvedExpr.flatMap(_.references).filter(!_.resolved).foreach {
        attr => {
          val failedMsg = exprName match {
            case Some(name) => s"${attr.sql} resolution in $name given these columns: "+
              planContaining.flatMap(_.output).map(_.name).mkString(",")
            case _ => s"${attr.sql} resolution failed given these columns: "+
              planContaining.flatMap(_.output).map(_.name).mkString(",")
          }
          attr.failAnalysis(failedMsg)
        }
      }
    }
    resolvedExpr
  }

  def hasSparkStopped(sparkSession: SparkSession): Boolean = {
    sparkSession.sparkContext.stopped.get()
  }

  /**
    * Qualify all the column names in the DF.
    * Attributes used in DF output will have fully qualified names
    * @param sparkSession
    * @param df DataFrame created by reading ACID table
    * @param fullyQualifiedTableName Qualified name of the Hive ACID Table
    * @return
    */
  def getDFQualified(sparkSession: SparkSession,
                     df: DataFrame,
                     fullyQualifiedTableName: String) = {
    val plan = df.queryExecution.analyzed
    val qualifiedPlan = plan match {
      case p: LogicalRelation =>
        p.copy(output = p.output
          .map((x: AttributeReference) =>
            x.withQualifier(fullyQualifiedTableName.split('.').toSeq))
        )
      case h: HiveTableRelation =>
        h.copy(dataCols = h.dataCols
          .map((x: AttributeReference) =>
            x.withQualifier(fullyQualifiedTableName.split('.').toSeq))
        )
        h.copy(partitionCols = h.partitionCols
          .map((x: AttributeReference) =>
            x.withQualifier(fullyQualifiedTableName.split('.').toSeq))
        )
      case _ => plan
    }

    val newDf = SqlUtils.convertToDF(sparkSession, qualifiedPlan)
    (qualifiedPlan, newDf)
  }

  def logicalPlanToDataFrame(sparkSession: SparkSession,
                             logicalPlan: LogicalPlan): DataFrame = {
    Dataset.ofRows(sparkSession, logicalPlan)
  }

  /**
    * Convert RDD into DataFrame using the attributeList.
    * Based on [[SparkSession.createDataFrame()]] implementation but here,
    * attributes are provided.
    * @param sparkSession
    * @param rdd
    * @param schema
    * @param attributes
    * @return
    */
  def createDataFrameUsingAttributes(sparkSession: SparkSession,
                                     rdd: RDD[Row],
                                     schema: StructType,
                                     attributes: Seq[Attribute]): DataFrame = {
    val encoder = RowEncoder(schema)
    val catalystRows = rdd.map(encoder.toRow)
    val logicalPlan = LogicalRDD(
      attributes,
      catalystRows,
      isStreaming = false)(sparkSession)
    Dataset.ofRows(sparkSession, logicalPlan)
  }

  def analysisException(cause: String): Throwable = {
    new AnalysisException(cause)
  }

  def removeTopSubqueryAlias(logicalPlan: LogicalPlan): LogicalPlan = {
    logicalPlan match {
      case SubqueryAlias(_, child: LogicalPlan) => child
      case _ => logicalPlan
    }
  }
}

case class FakeLogicalPlan(expr: Expression, children: Seq[LogicalPlan])
  extends LogicalPlan {
  override def output: Seq[Attribute] = children.foldLeft(Seq[Attribute]())((out, child) => out ++ child.output)
}
