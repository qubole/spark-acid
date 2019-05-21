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

import java.util.{List, Locale}

import com.qubole.shaded.hive.conf.HiveConf
import com.qubole.shaded.hive.metastore.HiveMetaStoreClient
import com.qubole.shaded.hive.metastore.api.{FieldSchema, Table}
import com.qubole.shaded.hive.metastore.utils.MetaStoreUtils.{getColumnNamesFromFieldSchema, getColumnTypesFromFieldSchema}
import com.qubole.shaded.hive.ql.metadata
import com.qubole.shaded.hive.ql.metadata.Hive
import com.qubole.shaded.hive.ql.plan.TableDesc
import com.qubole.spark.rdd.HiveRDD
import com.qubole.spark.util.{SerializableConfiguration, Util}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.InputFormat
import org.apache.spark.SparkException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{DataType, HIVE_TYPE_STRING, Metadata, MetadataBuilder, StructField, StructType}
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

  val cols: scala.List[FieldSchema] = table.getSd.getCols.toList
  val partitionCols: scala.List[FieldSchema] = table.getPartitionKeys.toList

  val dataSchema = StructType(cols.map(fromHiveColumn).toArray)
  val partitionSchema = StructType(partitionCols.map(fromHiveColumn).toArray)
  val broadcastedHadoopConf: Broadcast[SerializableConfiguration] = sqlContext.sparkContext.broadcast(
    new SerializableConfiguration(sqlContext.sparkContext.hadoopConfiguration))



  val acidState = new HiveAcidFileIndex(sqlContext.sparkSession, table,
    sqlContext.sparkSession.sessionState.conf.defaultSizeInBytes, client, partitionSchema)


  registerQEListener(sqlContext)
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

  private def registerQEListener(sqlContext: SQLContext): Unit = {
    sqlContext.sparkSession.listenerManager.register(new QueryExecutionListener {
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        println("Somani job end, closing _acid_state");
        if (acidState != null) {
          acidState.close()
        }
      }

      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        println("Somani job end, closing _acid_state");
        if (acidState != null) {
          acidState.close()
        }
      }
    })
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
    val initializeJobConfFunc = HiveAcidDataSource.initializeLocalJobConfFunc(
      hTable.getPath.toString , tableDesc) _
    val ifcName = hTable.getInputFormatClass.getName.replaceFirst(
      "org.apache.hadoop.hive.", "com.qubole.shaded.hive.")
    val ifc = Util.classForName(ifcName).asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
    val _minSplitsPerRDD = math.max(
      sqlContext.sparkContext.hadoopConfiguration.getInt("mapreduce.job.maps", 1),
      sqlContext.sparkContext.defaultMinPartitions)
    val deserializerClassName = tableDesc.getSerdeClassName.replaceFirst(
      "org.apache.hadoop.hive.", "com.qubole.shaded.hive.")
    val colNames = getColumnNamesFromFieldSchema(hTable.getSd.getCols)
    val colTypes = getColumnTypesFromFieldSchema(hTable.getSd.getCols)

    val allHiveCols = hTable.getAllCols
    val requiredHiveFields = requiredColumns.map(x => allHiveCols.find(_.getName == x).get)

    val dataTypes = requiredHiveFields.map {
      case x => getSparkSQLDataType(x)
    }
    val mutableRow = new SpecificInternalRow(dataTypes)
    val attrsWithIndex = requiredHiveFields.map { x => UnresolvedAttribute(x.getName)}.zipWithIndex

    new HiveRDD(sqlContext.sparkContext,
      acidState,
      broadcastedHadoopConf,
      Some(initializeJobConfFunc),
      ifc,
      new java.lang.Integer(_minSplitsPerRDD),
      deserializerClassName, mutableRow, attrsWithIndex, tableDesc.getProperties,
      colNames, colTypes
    )
  }.asInstanceOf[RDD[Row]]

}
