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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{AnalysisException, functions}

class MergeClauseSuite extends SparkFunSuite {
  def insertClause(addCondition : Boolean = true): MergeWhenNotInsert = {
    if (addCondition) {
      MergeWhenNotInsert(Some(functions.expr("x > 2").expr),
        Seq(functions.col("x").expr, functions.col("y").expr))
    }
    else {
      MergeWhenNotInsert(None,
        Seq(functions.col("x").expr, functions.col("y").expr))
    }
  }

  def updateClause(addCondition : Boolean = true): MergeWhenUpdateClause = {
    if (addCondition) {
      val updateCondition = Some(functions.expr("a > 2").expr)
      MergeWhenUpdateClause(updateCondition,
        Map("b" -> functions.lit(3).expr), isStar = false)
    } else {
      MergeWhenUpdateClause(None,
        Map("b" -> functions.lit(3).expr), isStar = false)
    }
  }

  def deleteClause(addCondition : Boolean = true): MergeWhenDelete = {
    if (addCondition) {
      MergeWhenDelete(Some(functions.expr("a < 1").expr))
    } else {
      MergeWhenDelete(None)
    }
  }

  test("Validate MergeClauses") {
    val clauses = Seq(insertClause(), updateClause(), deleteClause())
    MergeWhenClause.validate(clauses)
  }

  test("Invalid MergeClause cases") {
    val invalidMerge = "MERGE Validation Error: "

    //empty clauses
    checkInvalidMergeClause(invalidMerge + MergeWhenClause.atleastOneClauseError, Seq())

    // multi update or insert clauses
    val multiUpdateClauses = Seq(updateClause(), updateClause(), insertClause())
    checkInvalidMergeClause(invalidMerge + MergeWhenClause.justOneClausePerTypeError, multiUpdateClauses)

    // multi match clauses with first clause without condition
    val invalidMultiMatch = Seq(updateClause(false), deleteClause())
    checkInvalidMergeClause(invalidMerge + MergeWhenClause.matchClauseConditionError, invalidMultiMatch)

    // invalid Update Clause
    val invalidUpdateClause = MergeWhenUpdateClause(None, Map(), isStar = false)
    val thrown = intercept[IllegalArgumentException] {
      MergeWhenClause.validate(Seq(invalidUpdateClause))
    }
    assert(thrown.getMessage === "UPDATE Clause in MERGE should have one or more SET Values")
  }

  private def checkInvalidMergeClause(invalidMessage: String, multiUpdateClauses: Seq[MergeWhenClause]) = {
    val thrown = intercept[AnalysisException] {
      MergeWhenClause.validate(multiUpdateClauses)
    }
    assert(thrown.message === invalidMessage)
  }
}
