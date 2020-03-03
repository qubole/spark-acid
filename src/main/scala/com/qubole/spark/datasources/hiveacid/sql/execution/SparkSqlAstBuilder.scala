package com.qubole.spark.datasources.hiveacid.sql.execution

import java.util.Locale

import com.qubole.spark.datasources.hiveacid.sql.HiveAnalysisException
import com.qubole.spark.datasources.hiveacid.sql.catalyst.parser.SqlHiveParser._
import com.qubole.spark.datasources.hiveacid.sql.catalyst.parser.{AstBuilder, SqlHiveParser}
import com.qubole.spark.datasources.hiveacid.sql.catalyst.plans.logical.{Delete, Update}
import org.antlr.v4.runtime.Token
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf

import scala.collection.JavaConverters._

/**
 * An adaptation of [[org.apache.spark.sql.execution.SparkSqlAstBuilder]]
 */
class SparkSqlAstBuilder(conf: SQLConf) extends AstBuilder(conf) {
  import org.apache.spark.sql.catalyst.parser.ParserUtils._

  /**
   * Type to keep track of a table header: (identifier, isTemporary, ifNotExists, isExternal).
   */
  type TableHeader = (TableIdentifier, Boolean, Boolean, Boolean)

  /**
   * Convert a table property list into a key-value map.
   * This should be called through [[visitPropertyKeyValues]] or [[visitPropertyKeys]].
   */
  override def visitTablePropertyList(
                                       ctx: TablePropertyListContext): Map[String, String] = withOrigin(ctx) {
    val properties = ctx.tableProperty.asScala.map { property =>
      val key = visitTablePropertyKey(property.key)
      val value = visitTablePropertyValue(property.value)
      key -> value
    }
    // Check for duplicate property names.
    checkDuplicateKeys(properties, ctx)
    properties.toMap
  }

  /**
   * A table property key can either be String or a collection of dot separated elements. This
   * function extracts the property key based on whether its a string literal or a table property
   * identifier.
   */
  override def visitTablePropertyKey(key: TablePropertyKeyContext): String = {
    if (key.STRING != null) {
      string(key.STRING)
    } else {
      key.getText
    }
  }

  /**
   * A table property value can be String, Integer, Boolean or Decimal. This function extracts
   * the property value based on whether its a string, integer, boolean or decimal literal.
   */
  override def visitTablePropertyValue(value: TablePropertyValueContext): String = {
    if (value == null) {
      null
    } else if (value.STRING != null) {
      string(value.STRING)
    } else if (value.booleanValue != null) {
      value.getText.toLowerCase(Locale.ROOT)
    } else {
      value.getText
    }
  }

  /**
   * Create a [[ScriptInputOutputSchema]].
   */
  override protected def withScriptIOSchema(
                                             ctx: QuerySpecificationContext,
                                             inRowFormat: RowFormatContext,
                                             recordWriter: Token,
                                             outRowFormat: RowFormatContext,
                                             recordReader: Token,
                                             schemaLess: Boolean): ScriptInputOutputSchema = {
    if (recordWriter != null || recordReader != null) {
      // TODO: what does this message mean?
      throw new ParseException(
        "Unsupported operation: Used defined record reader/writer classes.", ctx)
    }

    // Decode and input/output format.
    type Format = (Seq[(String, String)], Option[String], Seq[(String, String)], Option[String])
    def format(
                fmt: RowFormatContext,
                configKey: String,
                defaultConfigValue: String): Format = fmt match {
      case c: RowFormatDelimitedContext =>
        // TODO we should use the visitRowFormatDelimited function here. However HiveScriptIOSchema
        // expects a seq of pairs in which the old parsers' token names are used as keys.
        // Transforming the result of visitRowFormatDelimited would be quite a bit messier than
        // retrieving the key value pairs ourselves.
        def entry(key: String, value: Token): Seq[(String, String)] = {
          Option(value).map(t => key -> t.getText).toSeq
        }
        val entries = entry("TOK_TABLEROWFORMATFIELD", c.fieldsTerminatedBy) ++
          entry("TOK_TABLEROWFORMATCOLLITEMS", c.collectionItemsTerminatedBy) ++
          entry("TOK_TABLEROWFORMATMAPKEYS", c.keysTerminatedBy) ++
          entry("TOK_TABLEROWFORMATLINES", c.linesSeparatedBy) ++
          entry("TOK_TABLEROWFORMATNULL", c.nullDefinedAs)

        (entries, None, Seq.empty, None)

      case c: RowFormatSerdeContext =>
        // Use a serde format.
        val CatalogStorageFormat(None, None, None, Some(name), _, props) = visitRowFormatSerde(c)

        // SPARK-10310: Special cases LazySimpleSerDe
        val recordHandler = if (name == "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe") {
          Option(conf.getConfString(configKey, defaultConfigValue))
        } else {
          None
        }
        (Seq.empty, Option(name), props.toSeq, recordHandler)

      case null =>
        // Use default (serde) format.
        val name = conf.getConfString("hive.script.serde",
          "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
        val props = Seq("field.delim" -> "\t")
        val recordHandler = Option(conf.getConfString(configKey, defaultConfigValue))
        (Nil, Option(name), props, recordHandler)
    }

    val (inFormat, inSerdeClass, inSerdeProps, reader) =
      format(
        inRowFormat, "hive.script.recordreader", "org.apache.hadoop.hive.ql.exec.TextRecordReader")

    val (outFormat, outSerdeClass, outSerdeProps, writer) =
      format(
        outRowFormat, "hive.script.recordwriter",
        "org.apache.hadoop.hive.ql.exec.TextRecordWriter")

    ScriptInputOutputSchema(
      inFormat, outFormat,
      inSerdeClass, outSerdeClass,
      inSerdeProps, outSerdeProps,
      reader, writer,
      schemaLess)
  }

  override def visitDeleteCommand(ctx: SqlHiveParser.DeleteCommandContext): LogicalPlan = visitDelete(ctx.delete)

  override def visitDelete(ctx: SqlHiveParser.DeleteContext): LogicalPlan = withOrigin(ctx) {
    val tableIdent = visitTableIdentifier(ctx.tableIdentifier)
    val relation = UnresolvedRelation(tableIdent)
    val condition = expression(ctx.where)
    subqueryNotSupportedCheck(Option(condition), "DELETE")
    Delete(relation, condition)
  }

  override def visitUpdateCommand(ctx: SqlHiveParser.UpdateCommandContext): LogicalPlan = visitUpdate(ctx.update)

  override def visitUpdate(ctx: SqlHiveParser.UpdateContext): LogicalPlan = withOrigin(ctx) {
    val fieldValues = visitUpdateFields(ctx.updateFieldList())
    val tableIdent = visitTableIdentifier(ctx.tableIdentifier)
    val relation = UnresolvedRelation(tableIdent)
    val condition = Option(ctx.where).map(expression)
    subqueryNotSupportedCheck(condition, "UPDATE")
    Update(relation, fieldValues, condition)
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
