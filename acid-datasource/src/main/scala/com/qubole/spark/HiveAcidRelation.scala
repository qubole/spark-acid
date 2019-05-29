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
import java.util.concurrent.TimeUnit

import com.qubole.shaded.hive.conf.HiveConf
import com.qubole.shaded.hive.metastore.HiveMetaStoreClient
import com.qubole.shaded.hive.metastore.api.{FieldSchema, Table}
import com.qubole.shaded.hive.ql.metadata
import com.qubole.shaded.hive.ql.metadata.Hive
import com.qubole.shaded.hive.ql.plan.TableDesc
import com.qubole.spark.rdd.HadoopTableReader
import com.qubole.spark.util.SerializableConfiguration
import org.apache.spark.SparkException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.PrettyAttribute
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConversions._
import scala.collection.mutable

class HiveAcidRelation(var sqlContext: SQLContext,
    parameters: Map[String, String])
    extends BaseRelation
    with PrunedFilteredScan
    with Logging {

  val tableName: String = parameters.getOrElse("table", {
    throw HiveAcidErrors.tableNotSpecifiedException
  })

  val hiveConf: HiveConf = HiveAcidDataSource.createHiveConf(sqlContext.sparkContext)
  //TODO: We should try to get rid of one of the below two clients (`client` and `hive`). They make two connections.
  val client = new HiveMetaStoreClient(hiveConf, null, false)
  val hive: Hive = Hive.get(hiveConf)
  val hTable: metadata.Table = hive.getTable(tableName.split('.')(0), tableName.split('.')(1))
  val table: Table  = client.getTable(tableName.split('.')(0), tableName.split('.')(1))

  if (table.getParameters.get("transactional") != "true") {
    throw HiveAcidErrors.tableNotAcidException
  }
  var isFullAcidTable: Boolean = table.getParameters.containsKey("transactional_properties") &&
    table.getParameters.get("transactional_properties").equals("insert_only")
  logInfo("Insert Only table: " + !isFullAcidTable)

  val dataSchema = StructType(table.getSd.getCols.toList.map(fromHiveColumn).toArray)
  val partitionSchema = StructType(table.getPartitionKeys.toList.map(fromHiveColumn).toArray)
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

  override val schema: StructType = {
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
    val partitionedHiveCols = hTable.getPartitionKeys
    val requiredHiveFields = requiredColumns.map(x => allHiveCols.find(_.getName == x).get)
    val attributes = requiredHiveFields.map { x =>
      PrettyAttribute(x.getName, getSparkSQLDataType(x))
    }
    val partitionAttributes = partitionedHiveCols.map { x =>
      PrettyAttribute(x.getName, getSparkSQLDataType(x))
    }

    val hadoopReader = new HadoopTableReader(
      attributes,
      partitionAttributes,
      tableDesc,
      sqlContext.sparkSession,
      acidState,
      sqlContext.sparkSession.sessionState.newHadoopConf())
    if (hTable.isPartitioned) {
      hadoopReader.makeRDDForPartitionedTable(getRawPartitions(filters)).asInstanceOf[RDD[Row]]
    } else {
      hadoopReader.makeRDDForTable(hTable).asInstanceOf[RDD[Row]]
    }
  }

  private def convertFilters(table: Table, filters: Seq[Filter]): String = {
    def convertInToOr(name: String, values: Seq[Any]): String = {
      values.map(value => s"$name = $value").mkString("(", " or ", ")")
    }

    def convert(filter: Filter): Option[String] = filter match {
      case In (name, values) =>
        Some(convertInToOr(name, values))

      case EqualTo(name, value) =>
        Some(s"$name = $value")

      case GreaterThan(name, value) =>
        Some(s"$name > $value")

      case GreaterThanOrEqual(name, value) =>
        Some(s"$name >= $value")

      case LessThan(name, value) =>
        Some(s"$name < $value")

      case LessThanOrEqual(name, value) =>
        Some(s"$name <= $value")

      case And(filter1, filter2) =>
        val converted = convert(filter1) ++ convert(filter2)
        if (converted.isEmpty) {
          None
        } else {
          Some(converted.mkString("(", " and ", ")"))
        }

      case Or(filter1, filter2) =>
        for {
          left <- convert(filter1)
          right <- convert(filter2)
        } yield s"($left or $right)"

      case _ => None
    }


    filters.flatMap(convert).mkString(" and ")
  }


  def getRawPartitions(filters: Array[Filter]): Seq[metadata.Partition] = {
    logWarning(s"total filters passed: ${filters.size}: $filters")
    val partitionKeyIds = hTable.getPartColNames.toSet
    val (pruningPredicates, otherPredicates) = filters.partition { predicate =>
      !predicate.references.isEmpty &&
        predicate.references.toSet.subsetOf(partitionKeyIds)
    }
    val prunedPartitions =
      if (sqlContext.sparkSession.sessionState.conf.metastorePartitionPruning &&
        pruningPredicates.size > 0) {
        val normalizedFilters = convertFilters(hTable.getTTable, pruningPredicates)
        hive.getPartitionsByFilter(hTable, normalizedFilters)
      } else {
        // sqlContext.sparkSession.sessionState.catalog.listPartitions(tIdentifier, None)
        hive.getPartitions(hTable)
      }
    logWarning(s"partition count = ${prunedPartitions.size()}")
    prunedPartitions.toSeq
  }


}
