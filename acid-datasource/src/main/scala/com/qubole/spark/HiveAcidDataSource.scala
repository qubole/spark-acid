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

import com.qubole.shaded.hive.conf.HiveConf
import com.qubole.shaded.hive.metastore.HiveMetaStoreClient
import com.qubole.shaded.hive.metastore.api.FieldSchema
import com.qubole.shaded.hive.metastore.utils.MetaStoreUtils.{getColumnNamesFromFieldSchema, getColumnTypesFromFieldSchema}
import com.qubole.shaded.hive.ql.exec.Utilities
import com.qubole.shaded.hive.ql.metadata.{Hive, HiveUtils, Table}
import com.qubole.shaded.hive.ql.plan.TableDesc
import com.qubole.spark.rdd.HiveRDD
import com.qubole.spark.util.SerializableConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf}
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructField, StructType, _}
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.JavaConversions._

class HiveAcidDataSource
  extends RelationProvider
    // with CreatableRelationProvider
    with PrunedFilteredScan
    with DataSourceRegister
    with Logging {

  var _acid_state: HiveAcidFileIndex = _
  var _client: HiveMetaStoreClient = _

  var _dataSchema: StructType = _
  var _partitionSchema: StructType = _

  var _sqlContext: SQLContext = _
  var _broadcastedHadoopConf: Broadcast[SerializableConfiguration] = _
  var _hTable: Table = _


  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {
    val tableName = parameters.getOrElse("table", {
      throw HiveAcidErrors.tableNotSpecifiedException
    })

    val hiveConf = HiveAcidDataSource.createHiveConf(sqlContext.sparkContext)
    _client = new HiveMetaStoreClient(hiveConf, null, false)
    val hive = Hive.get(hiveConf)

    val table = _client.getTable(tableName.split('.')(0), tableName.split('.')(1))
    _hTable = hive.getTable(tableName.split('.')(0), tableName.split('.')(1))

    val cols = table.getSd.getCols
    val partitionCols = table.getPartitionKeys

    _dataSchema = StructType(cols.toList.map(fromHiveColumn).toArray)
    _partitionSchema = StructType(partitionCols.map(fromHiveColumn).toArray)
    _sqlContext = sqlContext
    _broadcastedHadoopConf = _sqlContext.sparkContext.broadcast(
      new SerializableConfiguration(_sqlContext.sparkContext.hadoopConfiguration))



    if (_acid_state == null) {
      _acid_state = new HiveAcidFileIndex(sqlContext.sparkSession, table,
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

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    val tableDesc = new TableDesc(_hTable.getInputFormatClass, _hTable.getOutputFormatClass,
      _hTable.getMetadata)
    val initializeJobConfFunc = HiveAcidDataSource.initializeLocalJobConfFunc(
      _hTable.getPath.toString , tableDesc) _
    val ifc = _hTable.getInputFormatClass
      .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
    val _minSplitsPerRDD = math.max(
      _sqlContext.sparkContext.hadoopConfiguration.getInt("mapreduce.job.maps", 1),
      _sqlContext.sparkContext.defaultMinPartitions)
    val deserializerClassName = tableDesc.getSerdeClassName.replaceFirst(
      "org.apache.hadoop.hive.", "com.qubole.shaded.hive.")
    val colNames = getColumnNamesFromFieldSchema(_hTable.getSd.getCols)
    val colTypes = getColumnTypesFromFieldSchema(_hTable.getSd.getCols)

    val allHiveCols = _hTable.getAllCols
    val requiredHiveFields = requiredColumns.map(x => allHiveCols.find(_.getName == x).get)

    val dataTypes = requiredHiveFields.map {
      case x => HiveAcidDataSource.getCatalystDatatypeFromHiveDatatype(x.getType)
    }
    val mutableRow = new SpecificInternalRow(dataTypes)
    val attrsWithIndex = requiredHiveFields.map { x => UnresolvedAttribute(x.getName)}.zipWithIndex

    new HiveRDD(_sqlContext.sparkContext,
      _broadcastedHadoopConf,
      Some(initializeJobConfFunc),
      ifc,
      new java.lang.Integer(_minSplitsPerRDD),
      deserializerClassName, mutableRow, attrsWithIndex, tableDesc.getProperties,
      colNames, colTypes
    )
  }.asInstanceOf[RDD[Row]]
}

object HiveAcidDataSource extends Logging {
  def initializeLocalJobConfFunc(path: String, tableDesc: TableDesc)(jobConf: JobConf) {
    FileInputFormat.setInputPaths(jobConf, Seq[Path](new Path(path)): _*)
    if (tableDesc != null) {
      HiveAcidDataSource.configureJobPropertiesForStorageHandler(tableDesc, jobConf, true)
      Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
    }
    val bufferSize = System.getProperty("spark.buffer.size", "65536")
    jobConf.set("io.file.buffer.size", bufferSize)
  }

  def configureJobPropertiesForStorageHandler(
                                               tableDesc: TableDesc, conf: Configuration, input: Boolean) {
    val property = tableDesc.getProperties.getProperty(
      org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE
    )
    val storageHandler =
      HiveUtils.getStorageHandler(conf, property)
    if (storageHandler != null) {
      val jobProperties = new java.util.LinkedHashMap[String, String]
      if (input) {
        storageHandler.configureInputJobProperties(tableDesc, jobProperties)
      } else {
        storageHandler.configureOutputJobProperties(tableDesc, jobProperties)
      }
      if (!jobProperties.isEmpty) {
        tableDesc.setJobProperties(jobProperties)
      }
    }
  }

  def getCatalystDatatypeFromHiveDatatype(hiveDatatype: String): DataType = {
    hiveDatatype match {
      case "" => IntegerType
      case _ => StringType
    }
  }

  def createHiveConf(sparkContext: SparkContext): HiveConf = {
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

}
