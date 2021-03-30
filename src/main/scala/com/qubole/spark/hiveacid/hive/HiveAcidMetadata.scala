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

import scala.collection.JavaConversions._
import scala.collection.mutable
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.Warehouse
import org.apache.hadoop.hive.ql.io.RecordIdentifier
import org.apache.hadoop.hive.ql.metadata
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.plan.TableDesc
import com.qubole.spark.hiveacid.util.Util
import com.qubole.spark.hiveacid.HiveAcidErrors
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{InputFormat, OutputFormat}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
 * Represents metadata for hive acid table and exposes API to perform operations on top of it
 * @param sparkSession - spark session object
 * @param fullyQualifiedTableName - the fully qualified hive acid table name
 */
class HiveAcidMetadata(sparkSession: SparkSession,
                       fullyQualifiedTableName: String) extends Logging {

  // hive conf
  private val hiveConf: HiveConf = HiveConverter.getHiveConf(sparkSession.sparkContext)

  // a hive representation of the table
  val hTable: metadata.Table = {
    val hive: Hive = Hive.get(hiveConf)
    val table = sparkSession.sessionState.sqlParser.parseTableIdentifier(fullyQualifiedTableName)
    val hTable = hive.getTable(
      table.database match {
        case Some(database) => database
        case None => HiveAcidMetadata.DEFAULT_DATABASE
      }, table.identifier)
    Hive.closeCurrent()
    hTable
  }

  if (hTable.getParameters.get("transactional") != "true") {
    throw HiveAcidErrors.tableNotAcidException(getFullyQualifiedName(hTable.getTTable))
  }

  val isFullAcidTable: Boolean = hTable.getParameters.containsKey("transactional_properties") &&
    !hTable.getParameters.get("transactional_properties").equals("insert_only")
  val isInsertOnlyTable: Boolean = !isFullAcidTable
  val isBucketed: Boolean = hTable.getBucketCols() != null && hTable.getBucketCols.size() > 0

  // Table properties
  val isPartitioned: Boolean = hTable.isPartitioned
  val rootPath: Path = hTable.getDataLocation
  val dbName: String = hTable.getDbName
  val tableName: String = hTable.getTableName
  val fullyQualifiedName: String = getFullyQualifiedName(hTable.getTTable)

  // Schema properties
  val dataSchema = StructType(hTable.getSd.getCols.toList.map(
    HiveConverter.getCatalystStructField).toArray)

  val partitionSchema = StructType(hTable.getPartitionKeys.toList.map(
    HiveConverter.getCatalystStructField).toArray)

  val tableSchema: StructType = {
    val overlappedPartCols = mutable.Map.empty[String, StructField]
    partitionSchema.foreach { partitionField =>
      if (dataSchema.exists(getColName(_) == getColName(partitionField))) {
        overlappedPartCols += getColName(partitionField) -> partitionField
      }
    }
    StructType(dataSchema.map(f => overlappedPartCols.getOrElse(getColName(f), f)) ++
      partitionSchema.filterNot(f => overlappedPartCols.contains(getColName(f))))
  }

  val tableSchemaWithRowId: StructType = {
    StructType(
      Seq(
        StructField(HiveAcidMetadata.rowIdCol, HiveAcidMetadata.rowIdSchema)
      ) ++ tableSchema.fields)
  }

  lazy val tableDesc: TableDesc = {
    val inputFormatClass: Class[InputFormat[Writable, Writable]] =
      Util.classForName(hTable.getInputFormatClass.getName,
        loadShaded = true).asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
    val outputFormatClass: Class[OutputFormat[Writable, Writable]] =
      Util.classForName(hTable.getOutputFormatClass.getName,
        loadShaded = true).asInstanceOf[java.lang.Class[OutputFormat[Writable, Writable]]]
    new TableDesc(
      inputFormatClass,
      outputFormatClass,
      hTable.getMetadata)
  }

  /**
    * Returns list of partitions satisfying partition predicates
    * @param partitionFilters - filters to apply
    */
  def getRawPartitions(partitionFilters: Option[String] = None): Seq[metadata.Partition] = {
    val hive: Hive = Hive.get(hiveConf)
    val prunedPartitions = try {
      partitionFilters match {
        case Some(filter) => hive.getPartitionsByFilter(hTable, filter)
        case None => hive.getPartitions(hTable)
      }
    } finally {
      Hive.closeCurrent()
    }
    logDebug(s"partition count = ${prunedPartitions.size()}")
    prunedPartitions.toSeq
  }

  private def getColName(field: StructField): String = {
    HiveAcidMetadata.getColName(sparkSession, field)
  }
  private def getFullyQualifiedName(tTable : org.apache.hadoop.hive.metastore.api.Table): String = {
    return Warehouse.getQualifiedName(tTable)
  }
}

object HiveAcidMetadata {
  val DEFAULT_DATABASE = "default"

  val rowIdCol = "rowId"
  val rowIdSchema: StructType = {
    StructType(
      RecordIdentifier.Field.values().map {
        field =>
          StructField(
            name = field.name(),
            dataType = HiveConverter.getCatalystType(field.fieldType.getTypeName),
            nullable = true)
      }
    )
  }

  def fromSparkSession(sparkSession: SparkSession,
                       fullyQualifiedTableName: String): HiveAcidMetadata = {
    new HiveAcidMetadata(
      sparkSession,
      fullyQualifiedTableName)
  }

  def getColName(sparkSession: SparkSession, field: StructField): String = {
    if (sparkSession.sessionState.conf.caseSensitiveAnalysis) {
      field.name
    } else {
      field.name.toLowerCase(Locale.ROOT)
    }
  }

  def getColNames(sparkSession: SparkSession, schema: StructType): Seq[String] = {
    schema.map(getColName(sparkSession, _))
  }
}
