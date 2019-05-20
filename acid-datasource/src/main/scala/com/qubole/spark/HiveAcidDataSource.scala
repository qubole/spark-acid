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

import java.util.Locale

import com.fasterxml.jackson.annotation.JsonIgnore
import com.qubole.shaded.hive.conf.HiveConf
import com.qubole.shaded.hive.metastore.HiveMetaStoreClient

import scala.util.{Failure, Success, Try}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationStart, SparkListenerJobEnd}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, HadoopFsRelation, TextBasedFileFormat}
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.hive.client.HiveClientImpl
import com.qubole.shaded.hive.metastore.api.Table
import com.qubole.shaded.hive.metastore.api.FieldSchema
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class HiveAcidDataSource
  extends RelationProvider
    // with CreatableRelationProvider
    // with PrunedFilteredScan
    with DataSourceRegister
    with Logging {

  var _acid_state: HiveAcidFileIndex = _
  var _client: HiveMetaStoreClient = _

  var _dataSchema: StructType = _
  var _partitionSchema: StructType = _


  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {
    val tableName = parameters.getOrElse("table", {
      throw HiveAcidErrors.tableNotSpecifiedException
    })

    _client = new HiveMetaStoreClient(new HiveConf(), null, false)

    val hTable: Table = _client.getTable(tableName.split('.')(0), tableName.split('.')(1))
    val cols = hTable.getSd.getCols
    val partitionCols = hTable.getPartitionKeys

    _dataSchema = StructType(cols.toList.map(fromHiveColumn).toArray)
    _partitionSchema = StructType(partitionCols.map(fromHiveColumn).toArray)


    if (_acid_state == null) {
      _acid_state = new HiveAcidFileIndex(sqlContext.sparkSession, hTable,
        sqlContext.sparkSession.sessionState.conf.defaultSizeInBytes, _client, _partitionSchema)
    }

    registerQEListener(sqlContext)

    HadoopFsRelation(
      _acid_state,
      partitionSchema = _partitionSchema,
      dataSchema = _dataSchema,
      bucketSpec = None,
      fileFormat = new TextFileFormat(),
      options = Map.empty)(sqlContext.sparkSession)
  }


  def registerQEListener(sqlContext: SQLContext): Unit = {
    sqlContext.sparkSession.listenerManager.register(new QueryExecutionListener {
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        // scalastyle:off println
        println("Somani job end, closing _acid_state");
        // scalastyle:on println
        if (_acid_state != null) {
          _acid_state.close()
        }
      }

      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        // scalastyle:off println
        println("Somani job end, closing _acid_state");
        // scalastyle:on println
        if (_acid_state != null) {
          _acid_state.close()
        }
      }
    })
  }

  override def shortName(): String = {
    HiveAcidUtils.NAME
  }

  def fromHiveColumn(hc: FieldSchema): StructField = {
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

  //  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
  //    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
  //    JDBCRDD.scanTable(
  //      sparkSession.sparkContext,
  //      schema,`
  //      requiredColumns,
  //      filters,
  //      parts,
  //      jdbcOptions).asInstanceOf[RDD[Row]]
  //  }
}
