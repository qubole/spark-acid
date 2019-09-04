package com.qubole.spark.datasources.hiveacid.sql.execution

import java.util.Locale

import com.qubole.spark.datasources.hiveacid.sql.HiveAnalysisException
import com.qubole.spark.datasources.hiveacid.sql.catalyst.parser.SqlHiveParser
import com.qubole.spark.datasources.hiveacid.sql.catalyst.parser.SqlHiveParser.{TablePropertyKeyContext, TablePropertyListContext, TablePropertyValueContext, UpdateFieldListContext}
import com.qubole.spark.datasources.hiveacid.sql.catalyst.plans.logical.{Delete, Update}
import org.antlr.v4.runtime.ParserRuleContext
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedExtractValue, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}

import scala.collection.JavaConverters._

class HiveSqlAstBuilder(conf: SQLConf) extends SparkSqlAstBuilder(conf) {

  import org.apache.spark.sql.catalyst.parser.ParserUtils._

  override def visitDeleteCommand(ctx: SqlHiveParser.DeleteCommandContext): LogicalPlan = visitDelete(ctx.delete)

  override def visitDelete(ctx: SqlHiveParser.DeleteContext): LogicalPlan = withOrigin(ctx) {
    val tableIdent = visitTableIdentifier(ctx.tableIdentifier)
    val relation = UnresolvedRelation(tableIdent)
    val condition = Option(ctx.where).map(expression)
    subqueryNotSupportedCheck(condition, "DELETE")
    val filter = condition.map(Filter(_, relation))
    Delete(relation, filter)
  }

  override def visitUpdateCommand(ctx: SqlHiveParser.UpdateCommandContext): LogicalPlan = visitUpdate(ctx.update)

  override def visitUpdate(ctx: SqlHiveParser.UpdateContext): LogicalPlan = withOrigin(ctx) {
    val fieldValues = visitUpdateFields(ctx.updateFieldList())
    val expressions = fieldValues.toSeq.flatMap { case (field, value) =>
      val fieldAttr = UnresolvedAttribute(field)
      Seq(fieldAttr, value)
    }
    val tableIdent = visitTableIdentifier(ctx.tableIdentifier)
    val relation = UnresolvedRelation(tableIdent)
    val condition = Option(ctx.where).map(expression)
    subqueryNotSupportedCheck(condition, "UPDATE")
    val filter = condition.map(Filter(_, relation))
    Update(relation, expressions, filter)
  }

  private def visitUpdateFields(ctx: UpdateFieldListContext): Map[String, Expression] = {
    val fieldValues = visitUpdateFieldList(ctx)
    val badFields = fieldValues.collect { case (field, null) => field }
    if (badFields.nonEmpty) {
      operationNotAllowed(
        s"Values must be specified for field(s): ${badFields.mkString("[", ",", "]")}", ctx)
    }
    for ((_, expr) <- fieldValues) {
      subqueryNotSupportedCheck(Some(expr), "UPDATE")
    }
    fieldValues
  }

  override def visitUpdateFieldList(
                                       ctx: UpdateFieldListContext): Map[String, Expression] = withOrigin(ctx) {
    val fieldValues = ctx.updateField().asScala.map { updateField =>
      val field = updateField.identifier.getText
      val value = expression(updateField.value)
      field -> value
    }
    // Check for duplicate field names.
    checkDuplicateKeys(fieldValues, ctx)
    fieldValues.toMap
  }

  private def subqueryNotSupportedCheck(expression: Option[Expression], op: String): Unit = {
    expression match {
      case Some(expr) if SubqueryExpression.hasSubquery(expr) =>
        throw new HiveAnalysisException(s"Subqueries are not supported in the $op (expression = ${expr.sql}).")
      case _ =>
    }
  }
}
