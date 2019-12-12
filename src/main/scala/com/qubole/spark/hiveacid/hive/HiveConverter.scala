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

package com.qubole.spark.hiveacid.hive

import java.util.Locale

import com.qubole.shaded.hadoop.hive.conf.HiveConf
import com.qubole.shaded.hadoop.hive.metastore.api.FieldSchema
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

/**
  * Encapsulates everything (extensions, workarounds, quirks) to handle the
  * SQL dialect conversion between catalyst and hive.
  */
private[hiveacid] object HiveConverter extends Logging {

  def getCatalystStructField(hc: FieldSchema): StructField = {
    val columnType = getCatalystType(hc.getType)
    val metadata = if (hc.getType != columnType.catalogString) {
      new MetadataBuilder().putString(HIVE_TYPE_STRING, hc.getType).build()
    } else {
      Metadata.empty
    }

    val field = StructField(
      name = hc.getName,
      dataType = columnType,
      nullable = true,
      metadata = metadata)
    Option(hc.getComment).map(field.withComment).getOrElse(field)
  }

  def getCatalystType(dataType: String): DataType = {
    try {
      CatalystSqlParser.parseDataType(dataType)
    } catch {
      case e: ParseException =>
        throw new SparkException("Cannot recognize hive type string: " + dataType, e)
    }
  }

  def getHiveConf(sparkContext: SparkContext): HiveConf = {
    val hiveConf = new HiveConf()
    (sparkContext.hadoopConfiguration.iterator().map(kv => kv.getKey -> kv.getValue)
      ++ sparkContext.getConf.getAll.toMap).foreach { case (k, v) =>
      logDebug(
        s"""
           |Applying Hadoop/Hive/Spark and extra properties to Hive Conf:
           |$k=${if (k.toLowerCase(Locale.ROOT).contains("password")) "xxx" else v}
         """.stripMargin)
      hiveConf.set(k, v)
    }
    hiveConf
  }

  /**
    * Escape special characters in SQL string literals.
    *
    * @param value The string to be escaped.
    * @return Escaped string.
    */
  private def escapeSql(value: String): String = {
    // TODO: how to handle null
    StringUtils.replace(value, "'", "''")
  }

  /**
    * Converts value to SQL expression.
    * @param value The value to be converted.
    * @return Converted value.
    */
  private def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case _ => value
  }

  /**
    * Turns a single Filter into a String representing a SQL expression.
    * Returns None for an unhandled filter.
    */
  def compileFilter(f: Filter): Option[String] = Option(x = f match {
    case EqualTo(attr, value) => s"$attr = ${compileValue(value)}"
    case EqualNullSafe(attr, value) =>
      val col = attr
      s"(NOT ($col != ${compileValue(value)} OR $col = 'NULL' OR " +
        s"${compileValue(value)} = 'NULL') OR " +
        s"($col = 'NULL' AND ${compileValue(value)} = 'NULL'))"
    case LessThan(attr, value) => s"$attr < ${compileValue(value)}"
    case GreaterThan(attr, value) => s"$attr > ${compileValue(value)}"
    case LessThanOrEqual(attr, value) => s"$attr <= ${compileValue(value)}"
    case GreaterThanOrEqual(attr, value) => s"$attr >= ${compileValue(value)}"
    case IsNull(attr) => s"$attr = 'NULL'"
    case IsNotNull(attr) => s"$attr != 'NULL'"
    case StringStartsWith(attr, value) => s"$attr LIKE '$value%'"
    case StringEndsWith(attr, value) => s"$attr LIKE '%$value'"
    case StringContains(attr, value) => s"$attr LIKE '%$value%'"
    case In(attr, value) => s"$attr IN (${compileValue(value)})"
    case Not(`f`) => compileFilter(f).map(p => s"(NOT ($p))").orNull
    case Or(f1, f2) =>
      // We can't compile Or filter unless both sub-filters are compiled successfully.
      // It applies too for the following And filter.
      // If we can make sure compileFilter supports all filters, we can remove this check.
      val or = Seq(f1, f2) flatMap compileFilter
      if (or.size == 2) {
        or.map(p => s"($p)").mkString(" OR ")
      } else null
    case And(f1, f2) =>
      val and = Seq(f1, f2).flatMap(compileFilter)
      if (and.size == 2) {
        and.map(p => s"($p)").mkString(" AND ")
      } else null
    case _ => null
  })


  def compileFilters(filters: Seq[Filter]): String = {
    val str = filters.flatMap(compileFilter).mkString(" and ")
    logDebug(str)
    str
  }
}
