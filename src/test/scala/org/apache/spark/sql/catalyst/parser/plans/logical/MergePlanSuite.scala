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

import java.util.Locale

import com.qubole.spark.hiveacid.merge.{MergeWhenClause, MergeWhenDelete, MergeWhenNotInsert, MergeWhenUpdateClause}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.{AnalysisException, functions}
import org.apache.spark.sql.test.SharedSQLContext

class MergePlanSuite extends AnalysisTest with SharedSQLContext {
  test("Resolve valid Merge Plans with all 3 clauses") {
    val sourceTable = "source"
    val targetTable = "target"
    withTable(sourceTable, targetTable) {
      new TestBuilder()
        .withMergeCondition("x.id = y.a")
        .withUpdateClause(Some("y.a < 100"), Map("x.id" -> "y.a", "x.value" -> "y.b"))
        .withDeleteClause(Some("y.a >= 100"))
        .withInsertClause(Some("y.a > 100"), Seq("y.a", "y.b"))
        .test()
    }
  }

  test("UPDATE SET without alias for " +
    "column being set and set expression using columns from both tables") {
    val sourceTable = "source"
    val targetTable = "target"
    withTable(sourceTable, targetTable) {
      new TestBuilder()
        .withMergeCondition("x.id = y.a")
        .withUpdateClause(Some("y.a < 100"), Map("id" -> "y.a + x.id",
          "value" -> "concat(y.b, x.value)"))
        .test()
    }
  }

  test("Insert with * used") {
    withTable("source", "target") {
      new TestBuilder().withTargetCols("id int, value string, event_type string")
        .withSourceCols("id int, value string")
        .withMergeCondition("x.id = y.id")
        .withInsertClause(Seq(UnresolvedStar(None), functions.lit("insert").expr), None)
        .test()
    }
  }

  test("Merge condition with Extra non-Join Clauses") {
    withTable("source", "target") {
      new TestBuilder()
        .withMergeCondition("x.id = y.a and x.id > 10 and y.b != 'test'")
        .withUpdateClause(Some("y.a < 100"), Map("id" -> "y.a + x.id",
          "value" -> "concat(y.b, x.value)"))
        .test()
    }
  }

  test("Delete condition on both the tables") {
    withTable("source", "target") {
      new TestBuilder()
        .withMergeCondition("x.id = y.a")
        .withDeleteClause(Some("y.b = x.value"))
        .test()
    }
  }

  test("Invalid Delete condition") {
    withTable("source", "target") {
      new TestBuilder()
        .withMergeCondition("x.id = y.a")
        .withDeleteClause(Some("y.b = x.wrong_id"))
        .testUnsupported(Seq("`x.wrong_id` resolution in mergeDeleteClause" +
          " given these columns: a,b,id,value;"))
    }
  }

  test("test insufficient insert columns") {
    // Just 2 insert values specified instead of 3
    withTable("source", "target") {
      new TestBuilder().withTargetCols("id int, value string, event_type string")
        .withSourceCols("id int, value string")
        .withMergeCondition("x.id = y.id")
        .withInsertClause(Seq(UnresolvedStar(None)), None)
        .testUnsupported()
    }
  }

  test("Insert with * on mismatched column names") {
    withTable("source", "target") {
      new TestBuilder()
        .withMergeCondition("x.id = y.a")
        .withInsertClause(Seq(UnresolvedStar(None)), None)
        .testUnsupported(Seq("mergeInsertClause " +
          "using `*` failed to resolve: column `a` is not present in target" +
          " columns. `*` denotes sequence of source attributes" +
          " expected to be present in target too"))
    }
  }

  test("Invalid Insert condition") {
    withTable("source", "target") {
      new TestBuilder().withTargetCols("id int, value string, event_type string")
        .withSourceCols("id int, value string")
        .withMergeCondition("x.id = y.id")
        .withInsertClause(Seq(UnresolvedStar(None), functions.lit("insert").expr), Some("x.wrong_id > 30"))
        .testUnsupported(Seq("x.wrong_id"))
    }
  }

  test("Invalid Delete condition on both the tables") {
    withTable("source", "target") {
      new TestBuilder()
        .withMergeCondition("x.id = y.a")
        .withDeleteClause(Some("y.wrong_b = x.value"))
        .testUnsupported(Seq("y.wrong_b"))
    }
  }

  private def createSourceTarget(sourceTable: String, targetTable: String,
                                 sourceCols: String, targetCols: String) = {
    sql(s"CREATE table $targetTable($targetCols) USING Parquet")
    sql(s"CREATE table $sourceTable($sourceCols) USING Parquet")
  }

  /**
    * Basic framework to test MergePlan. Following are provided
    * a. source table named `source` aliased by y
    * b. target table named `table` aliased by x
    * c. ability to add the clauses
    * d. Default target table columns are (id int, value string). override using
    * e. Default source table columns  (a int, b string)
    */
  private class TestBuilder {
    var sourceCols = "a int, b string"
    var targetCols = "id int, value string"
    val sourcePlan = SubqueryAlias("y", UnresolvedRelation(TableIdentifier("source")))
    val targetPlan = SubqueryAlias("x", UnresolvedRelation(TableIdentifier("target")))
    var expr: Expression = null
    var matchedClause = Seq[MergeWhenClause]()
    var nonMatched: Option[MergeWhenClause] = None

    def withSourceCols(srcCols: String): TestBuilder = {
      sourceCols = srcCols
      this
    }

    def withTargetCols(tgtCols: String): TestBuilder = {
      targetCols = tgtCols
      this
    }
    def withMergeCondition(cond: String): TestBuilder = {
      expr = functions.expr(cond).expr
      this
    }
    def withDeleteClause(cond: Option[String]): TestBuilder = {
      matchedClause = matchedClause ++ Seq(MergeWhenDelete(cond.map(functions.expr(_).expr)))
      this
    }

    def withUpdateClause(cond: Option[String], setExp: Map[String, String]): TestBuilder = {
      val matchCondition = cond.map(functions.expr(_).expr)
      val setExpression = setExp.mapValues(expStr => functions.expr(expStr).expr)
      matchedClause = matchedClause ++ Seq(MergeWhenUpdateClause(matchCondition, setExpression, false))
      this
    }

    def withInsertClause(cond: Option[String], exprStrs: Seq[String]): TestBuilder = {
      val matchingCondition = cond.map(functions.expr(_).expr)
      val expressions = exprStrs.map(functions.expr(_).expr)
      nonMatched = Some(MergeWhenNotInsert(matchingCondition, expressions))
      this
    }

    def withInsertClause(expressions: Seq[Expression], cond: Option[String]): TestBuilder = {
      val matchingCondition = cond.map(functions.expr(_).expr)
      nonMatched = Some(MergeWhenNotInsert(matchingCondition, expressions))
      this
    }

    def test(): Unit = {
      createSourceTarget("source", "target", sourceCols, targetCols)
      val mergePlan = MergePlan(sourcePlan, targetPlan, expr, matchedClause, nonMatched)
      val analyzer = spark.sessionState.analyzer
      val analyzedMergePlan = analyzer.execute(mergePlan)
      checkAnalyzedPlan(mergePlan, analyzedMergePlan)
      val resolvedMergePlan = MergePlan.resolve(spark, analyzedMergePlan.asInstanceOf[MergePlan])
      checkResolvedMerge(resolvedMergePlan)
    }

    def testUnsupported(containsThesePhrases: Seq[String] = Seq()): Unit = {
      createSourceTarget("source", "target", sourceCols, targetCols)
      val mergePlan = MergePlan(sourcePlan, targetPlan, expr, matchedClause, nonMatched)
      val analyzer = spark.sessionState.analyzer
      val analyzedMergePlan = analyzer.execute(mergePlan)
      checkAnalyzedPlan(mergePlan, analyzedMergePlan)
      val e = intercept[AnalysisException] {
        MergePlan.resolve(spark, analyzedMergePlan.asInstanceOf[MergePlan])
      }
      containsThesePhrases.foreach { p =>
        assert(e.getMessage.toLowerCase(Locale.ROOT).contains(p.toLowerCase(Locale.ROOT)))
      }
    }
  }

  private def checkResolvedMerge(resolvedMergePlan: MergePlan): Unit = {
    (resolvedMergePlan.matched ++ resolvedMergePlan.notMatched).foreach {
      case MergeWhenUpdateClause(cond, setExpr, _) => {
        cond.map(expr => assert(expr.resolved))
        setExpr.values.map(expr => assert(expr.resolved))
      }
      case MergeWhenDelete(cond) => cond.map(expr => assert(expr.resolved))
      case MergeWhenNotInsert(cond, exprs) => {
        cond.map(expr => assert(expr.resolved))
        exprs.map(expr => assert(expr.resolved))
      }
    }
  }

  def checkAnalyzedPlan(originalPlan: LogicalPlan, analyzedPlan: LogicalPlan): Unit = {
    try spark.sessionState.analyzer.checkAnalysis(analyzedPlan) catch {
      case a: AnalysisException =>
        fail(
          s"""
             |Failed to Analyze Plan
             |$originalPlan
             |
             |Partial Analysis
             |$analyzedPlan
                """.stripMargin, a)
    }
  }
}