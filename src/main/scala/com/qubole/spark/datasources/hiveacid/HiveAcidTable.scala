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

package com.qubole.spark.datasources.hiveacid

import java.util.Locale

import com.qubole.shaded.hadoop.hive.conf.HiveConf
import com.qubole.shaded.hadoop.hive.metastore.api.FieldSchema
import com.qubole.shaded.hadoop.hive.ql.metadata
import com.qubole.shaded.hadoop.hive.ql.metadata.Hive
import com.qubole.shaded.hadoop.hive.ql.plan.TableDesc
import com.qubole.spark.datasources.hiveacid.util.Util
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{InputFormat, OutputFormat}
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.mutable

class HiveAcidTable(val hTable: metadata.Table) extends Logging {

  if (hTable.getParameters.get("transactional") != "true") {
    throw HiveAcidErrors.tableNotAcidException
  }

  val isFullAcidTable: Boolean = hTable.getParameters.containsKey("transactional_properties") &&
    !hTable.getParameters.get("transactional_properties").equals("insert_only")
  val isPartitioned: Boolean = hTable.isPartitioned
  val rootPath: Path = hTable.getDataLocation
  val dbName: String = hTable.getDbName
  val tableName: String = hTable.getTableName
  val fullyQualifiedName: String = hTable.getFullyQualifiedName
  lazy val inputFormatClass: Class[InputFormat[Writable, Writable]] =
    Util.classForName(hTable.getInputFormatClass.getName,
      true).asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]

  lazy val outputFormatClass: Class[OutputFormat[Writable, Writable]] =
    Util.classForName(hTable.getOutputFormatClass.getName,
      true).asInstanceOf[java.lang.Class[OutputFormat[Writable, Writable]]]

  lazy val tableDesc: TableDesc = new TableDesc(
    inputFormatClass,
    outputFormatClass,
    hTable.getMetadata)

  val dataSchema = StructType(hTable.getSd.getCols.toList.map(fromHiveColumn).toArray)
  val partitionSchema = StructType(hTable.getPartitionKeys.toList.map(fromHiveColumn).toArray)

  val schema: StructType = {
    val overlappedPartCols = mutable.Map.empty[String, StructField]
    partitionSchema.foreach { partitionField =>
      if (dataSchema.exists(getColName(_) == getColName(partitionField))) {
        overlappedPartCols += getColName(partitionField) -> partitionField
      }
    }
    StructType(dataSchema.map(f => overlappedPartCols.getOrElse(getColName(f), f)) ++
      partitionSchema.filterNot(f => overlappedPartCols.contains(getColName(f))))
  }

  private def getColName(f: StructField): String = {
    //    if (sqlContext.sparkSession.sessionState.conf.caseSensitiveAnalysis) {
    //      f.name
    //    } else {
    //      f.name.toLowerCase(Locale.ROOT)
    //    }
    f.name.toLowerCase(Locale.ROOT)
  }

  private def fromHiveColumn(hc: FieldSchema): StructField = {
    val columnType = getSparkSQLDataType(hc)
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

  private def getSparkSQLDataType(hc: FieldSchema): DataType = {
    try {
      CatalystSqlParser.parseDataType(hc.getType)
    } catch {
      case e: ParseException =>
        throw new SparkException("Cannot recognize hive type string: " + hc.getType, e)
    }
  }
}

object HiveAcidTable {
  def fromTableName(fullyQualifiedTableName: String, hiveConf: HiveConf): HiveAcidTable = {
    // Currently we are creating and closing a connection to the hive metastore every
    // time we need to do something. This can be optimized.
    val hive: Hive = Hive.get(hiveConf)
    val hTable = hive.getTable(fullyQualifiedTableName.split('.')(0),
      fullyQualifiedTableName.split('.')(1))
    Hive.closeCurrent()
    new HiveAcidTable(hTable)
  }
}