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

package com.qubole.spark

import java.util.concurrent.TimeUnit
import java.util.{List, Locale}

import com.qubole.shaded.hive.conf.HiveConf
import com.qubole.shaded.hive.metastore.HiveMetaStoreClient
import com.qubole.shaded.hive.metastore.api.{FieldSchema, Table}
import com.qubole.shaded.hive.metastore.utils.MetaStoreUtils.{getColumnNamesFromFieldSchema, getColumnTypesFromFieldSchema}
import com.qubole.shaded.hive.ql.metadata
import com.qubole.shaded.hive.ql.metadata.Hive
import com.qubole.shaded.hive.ql.plan.TableDesc
import com.qubole.spark.rdd.{HadoopTableReader, Hive3RDD}
import com.qubole.spark.util.{SerializableConfiguration, Util}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.InputFormat
import org.apache.spark.SparkException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{PrettyAttribute, SpecificInternalRow}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.execution.{QueryExecution, RowDataSourceScanExec}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.mutable
import scala.collection.JavaConversions._

class HiveAcidRelation(var sqlContext: SQLContext,
    parameters: Map[String, String])
    extends BaseRelation
    with PrunedFilteredScan
    with Logging {

  val tableName: String = parameters.getOrElse("table", {
    throw HiveAcidErrors.tableNotSpecifiedException
  })

  val hiveConf: HiveConf = HiveAcidDataSource.createHiveConf(sqlContext.sparkContext)
  val client = new HiveMetaStoreClient(hiveConf, null, false)
  val hive: Hive = Hive.get(hiveConf)
  val hTable: metadata.Table = hive.getTable(tableName.split('.')(0), tableName.split('.')(1))


  val table: Table  = client.getTable(tableName.split('.')(0), tableName.split('.')(1))

  if (table.getParameters.get("transactional") != "true") {
    throw HiveAcidErrors.tableNotAcidException
  }
  var isFullAcidTable: Boolean = _
  // 'transactional_properties'='insert_only'
  if (table.getParameters.containsKey("transactional_properties") &&
    (table.getParameters.get("transactional_properties") == "insert_only")) {
    isFullAcidTable = false
  } else {
    isFullAcidTable = true
  }
  logInfo("Somani: insert_only: " + !isFullAcidTable)


  val cols: scala.List[FieldSchema] = table.getSd.getCols.toList
  val partitionCols: scala.List[FieldSchema] = table.getPartitionKeys.toList

  val dataSchema = StructType(cols.map(fromHiveColumn).toArray)
  val partitionSchema = StructType(partitionCols.map(fromHiveColumn).toArray)
  val broadcastedHadoopConf: Broadcast[SerializableConfiguration] = sqlContext.sparkContext.broadcast(
    new SerializableConfiguration(sqlContext.sparkContext.hadoopConfiguration))

  val acidState = new HiveAcidState(sqlContext.sparkSession, hiveConf, table,
    sqlContext.sparkSession.sessionState.conf.defaultSizeInBytes, client, partitionSchema,
  HiveConf.getTimeVar(hiveConf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS) / 2, isFullAcidTable)
  
  val overlappedPartCols = mutable.Map.empty[String, StructField]

  partitionSchema.foreach { partitionField =>
    if (dataSchema.exists(getColName(_) == getColName(partitionField))) {
      overlappedPartCols += getColName(partitionField) -> partitionField
    }
  }
  val schema: StructType = {
    StructType(dataSchema.map(f => overlappedPartCols.getOrElse(getColName(f), f)) ++
      partitionSchema.filterNot(f => overlappedPartCols.contains(getColName(f))))
  }

  override def sizeInBytes: Long = {
    val compressionFactor = sqlContext.sparkSession.sessionState.conf.fileCompressionFactor
    (acidState.sizeInBytes * compressionFactor).toLong
  }

  override val needConversion: Boolean = false

  private def getColName(f: StructField): String = {
    if (sqlContext.sparkSession.sessionState.conf.caseSensitiveAnalysis) {
      f.name
    } else {
      f.name.toLowerCase(Locale.ROOT)
    }
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

  /** Get the Spark SQL native DataType from Hive's FieldSchema. */
  private def getSparkSQLDataType(hc: FieldSchema): DataType = {
    try {
      CatalystSqlParser.parseDataType(hc.getType)
    } catch {
      case e: ParseException =>
        throw new SparkException("Cannot recognize hive type string: " + hc.getType, e)
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val tableDesc = new TableDesc(hTable.getInputFormatClass, hTable.getOutputFormatClass,
      hTable.getMetadata)
    val allHiveCols = hTable.getAllCols
    val requiredHiveFields = requiredColumns.map(x => allHiveCols.find(_.getName == x).get)
    val attributes = requiredHiveFields.map { x =>
      PrettyAttribute(x.getName, getSparkSQLDataType(x))
    }

    // TODO: Add support for partitioning
    val partCols = Seq()

    val hadoopReader = new HadoopTableReader(
      attributes,
      partCols,
      tableDesc,
      sqlContext.sparkSession,
      acidState,
      sqlContext.sparkSession.sessionState.newHadoopConf())
    hadoopReader.makeRDDForTable(hTable).asInstanceOf[RDD[Row]]
  }

}
