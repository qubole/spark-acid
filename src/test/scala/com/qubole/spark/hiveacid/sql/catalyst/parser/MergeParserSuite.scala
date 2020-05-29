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

package com.qubole.spark.hiveacid.sql.catalyst.parser

import java.util.Locale

import com.qubole.spark.datasources.hiveacid.sql.catalyst.plans.command.MergeCommand
import com.qubole.spark.datasources.hiveacid.sql.execution.SparkAcidSqlParser
import com.qubole.spark.hiveacid.merge.{MergeCondition, MergeWhenDelete, MergeWhenNotInsert, MergeWhenUpdateClause}
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.functions
import org.apache.spark.sql.internal.SQLConf

class MergeParserSuite extends PlanTest {
  private lazy val parser = SparkAcidSqlParser(new SparkSqlParser(new SQLConf))

  private def assertUnsupported(sql: String, containsThesePhrases: Seq[String] = Seq()): Unit = {
    val e = intercept[ParseException] {
      parser.parsePlan(sql)
    }
    containsThesePhrases.foreach { p =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(p.toLowerCase(Locale.ROOT)))
    }
  }

  test("Parse Valid MERGE Statements") {
    val sql1 =
      """
        |MERGE INTO target_table t USING source_table s
        |ON t.id = s.id
        |WHEN MATCHED AND t.id <= 10 THEN UPDATE SET city = s.city
        |WHEN MATCHED AND t.id > 10 THEN DELETE
        |WHEN NOT MATCHED AND s.id > 100 THEN INSERT VALUES (*)
        |""".stripMargin
    val parsed1 = parser.parsePlan(sql1)
    val expected1 = MergeCommand(
      SubqueryAlias("t", UnresolvedRelation(TableIdentifier("target_table"))),
      SubqueryAlias("s", UnresolvedRelation(TableIdentifier("source_table"))),
      Seq(MergeWhenUpdateClause(Some(functions.expr("t.id <= 10").expr),
        Map("city" -> UnresolvedAttribute("s.city")), isStar = false),
        MergeWhenDelete(Some(functions.expr("t.id > 10").expr))),
      Some(MergeWhenNotInsert(Some(functions.expr("s.id > 100").expr),
        Seq(UnresolvedStar(None)))),
      MergeCondition(functions.expr("t.id = s.id").expr),
      Some(AliasIdentifier("s")),
      Some(AliasIdentifier("t")))

    val sql2 = """
                 |MERGE INTO target_table USING source_table
                 |ON id = out
                 |WHEN MATCHED AND id <= 10 THEN UPDATE SET city = out_city
                 |WHEN MATCHED AND id > 10 THEN DELETE
                 |WHEN NOT MATCHED AND id > 100 THEN INSERT VALUES (*)
                 |""".stripMargin
    val parsed2 = parser.parsePlan(sql2)
    val expected2 = MergeCommand(
      UnresolvedRelation(TableIdentifier("target_table")),
      UnresolvedRelation(TableIdentifier("source_table")),
      Seq(MergeWhenUpdateClause(Some(functions.expr("id <= 10").expr),
        Map("city" -> UnresolvedAttribute("out_city")), isStar = false),
        MergeWhenDelete(Some(functions.expr("id > 10").expr))),
      Some(MergeWhenNotInsert(Some(functions.expr("id > 100").expr),
        Seq(UnresolvedStar(None)))),
      MergeCondition(functions.expr("id = out").expr),
      None, None)

    val sql3 =
      """
        |MERGE INTO target_table t USING source_table s
        |ON t.id = s.id
        |WHEN MATCHED AND t.id <= 10 THEN UPDATE SET city = s.city
        |WHEN MATCHED AND t.id > 10 THEN DELETE
        |WHEN NOT MATCHED AND s.id > 100 THEN INSERT VALUES (s.id, *, s.city)
        |""".stripMargin
    val parsed3 = parser.parsePlan(sql3)
    val expected3 = MergeCommand(
      SubqueryAlias("t", UnresolvedRelation(TableIdentifier("target_table"))),
      SubqueryAlias("s", UnresolvedRelation(TableIdentifier("source_table"))),
      Seq(MergeWhenUpdateClause(Some(functions.expr("t.id <= 10").expr),
        Map("city" -> UnresolvedAttribute("s.city")), isStar = false),
        MergeWhenDelete(Some(functions.expr("t.id > 10").expr))),
      Some(MergeWhenNotInsert(Some(functions.expr("s.id > 100").expr),
        Seq(UnresolvedAttribute("s.id"), UnresolvedStar(None),
          UnresolvedAttribute("s.city")))),
      MergeCondition(functions.expr("t.id = s.id").expr),
      Some(AliasIdentifier("s")),
      Some(AliasIdentifier("t")))
    val sql4 =
      """
        |MERGE INTO target_table t USING source_table s
        |ON t.id = s.id
        |WHEN MATCHED AND t.id <= 10 THEN UPDATE SET t.city = s.city
        |WHEN MATCHED AND t.id > 10 THEN DELETE
        |WHEN NOT MATCHED AND s.id > 100 THEN INSERT VALUES (s.id, *, s.city)
        |""".stripMargin
    val expected4 = MergeCommand(
      SubqueryAlias("t", UnresolvedRelation(TableIdentifier("target_table"))),
      SubqueryAlias("s", UnresolvedRelation(TableIdentifier("source_table"))),
      Seq(MergeWhenUpdateClause(Some(functions.expr("t.id <= 10").expr),
        Map("t.city" -> UnresolvedAttribute("s.city")), isStar = false),
        MergeWhenDelete(Some(functions.expr("t.id > 10").expr))),
      Some(MergeWhenNotInsert(Some(functions.expr("s.id > 100").expr),
        Seq(UnresolvedAttribute("s.id"), UnresolvedStar(None),
          UnresolvedAttribute("s.city")))),
      MergeCondition(functions.expr("t.id = s.id").expr),
      Some(AliasIdentifier("s")),
      Some(AliasIdentifier("t")))
    comparePlans(parsed1, expected1, checkAnalysis = false)
    comparePlans(parsed2, expected2, checkAnalysis = false)
    comparePlans(parsed3, expected3, checkAnalysis = false)
    //TODO: To support alias in the set columns of update
    //val parsed4 = parser.parsePlan(sql4)
    //comparePlans(parsed4, expected4, checkAnalysis = false)
  }

  test("Parse Invalid Merge Statement") {
    val sql1 =
      """
        |MERGE INTO target_table t USING source_table s
        |ON t.id = s.id
        |WHEN MATCHED AND t.id <= 10 UPDATE SET city = s.city
        |WHEN MATCHED AND t.id > 10 DELETE
        |WHEN NOT MATCHED AND s.id > 100 INSERT VALUES (*)
        |""".stripMargin
    assertUnsupported(sql1, Seq("missing 'THEN' at 'UPDATE'(line 4, pos 28)"))
    val sql2 =
      """
        |MERGE INTO target_table t USING source_table s
        |ON t.id = s.id
        |""".stripMargin
    assertUnsupported(sql2,
      Seq("mismatched input '<EOF>' expecting {'WHEN', 'ELSE'}(line 4, pos 0)"))
    val sql3 =
      """
        |MERGE INTO target_table t USING source_table s
        |WHEN MATCHED AND t.id <= 10 UPDATE SET city = s.city
        |WHEN MATCHED AND t.id > 10 DELETE
        |WHEN NOT MATCHED AND s.id > 100 INSERT VALUES (*)
        |""".stripMargin
    assertUnsupported(sql3, Seq("missing 'ON' at 'WHEN'(line 3, pos 0)"))
    val sql4 =
      """
        |MERGE INTO target_table t USING source_table s
        |ON t.id = s.id
        |WHEN MATCHED AND t.id <= 10 THEN UPDATE SET count = (SELECT count(*) from foo)
        |WHEN MATCHED AND t.id > 10 THEN DELETE
        |WHEN NOT MATCHED AND s.id > 100 THEN INSERT VALUES (*)
        |""".stripMargin
    assertUnsupported(sql4,
      Seq("Subqueries are not supported in the UPDATE (expression = scalarsubquery())."))
    val sql5 =
      """
        |MERGE INTO target_table t USING source_table s
        |ON t.id = s.id
        |WHEN MATCHED AND t.id <= 10 THEN UPDATE SET city = s.city
        |WHEN MATCHED AND t.id > 10 THEN DELETE
        |WHEN NOT MATCHED AND s.id > 100 THEN INSERT VALUES ((SELECT count(*) from foo), *)
        |""".stripMargin
    assertUnsupported(sql5,
      Seq("Subqueries are not supported in the INSERT clause of MERGE (expression = scalarsubquery())."))
    val sql6 =
      """
        |MERGE INTO target_table t USING source_table s
        |ON t.id = s.id AND t.part in (SELECT distinct part from foo)
        |WHEN MATCHED AND t.id <= 10 THEN UPDATE SET city = s.city
        |WHEN MATCHED AND t.id > 10 THEN DELETE
        |WHEN NOT MATCHED AND s.id > 100 THEN INSERT VALUES (*)
        |""".stripMargin
    assertUnsupported(sql6, Seq("Subqueries are not supported in the ON condition in MERGE" +
      " (expression = ((`t.id` = `s.id`) AND (`t.part` IN (listquery()))))."))
  }
}
