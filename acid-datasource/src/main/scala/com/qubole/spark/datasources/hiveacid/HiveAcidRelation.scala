/*
 * Copyright 2019 Qubole, Inc.  All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the “License”); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qubole.spark.datasources.hiveacid

import java.util.Locale
import java.util.concurrent.TimeUnit

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.qubole.shaded.hadoop.hive.conf.HiveConf
import com.qubole.shaded.hadoop.hive.metastore.api.{FieldSchema, Table}
import com.qubole.shaded.hadoop.hive.ql.metadata
import com.qubole.shaded.hadoop.hive.ql.metadata.Hive
import com.qubole.shaded.hadoop.hive.ql.plan.TableDesc
import com.qubole.shaded.orc.mapreduce.OrcInputFormat
import com.qubole.spark.datasources.hiveacid.orc.OrcFilters
import com.qubole.spark.datasources.hiveacid.rdd.HiveTableReader
import com.qubole.shaded.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils
import org.apache.spark.{SparkContext, SparkException}
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

  private val tableName: String = parameters.getOrElse("table", {
    throw HiveAcidErrors.tableNotSpecifiedException
  })

  private val hiveConf: HiveConf = HiveAcidRelation.createHiveConf(sqlContext.sparkContext)

  private val hTable: metadata.Table = {
    // Currently we are creating and closing a connection to the hive metastore every time we need to do something.
    // This can be optimized.
    val hive: Hive = Hive.get(hiveConf)
    val hTable = hive.getTable(tableName.split('.')(0), tableName.split('.')(1))
    Hive.closeCurrent()
    hTable
  }

  if (hTable.getParameters.get("transactional") != "true") {
    throw HiveAcidErrors.tableNotAcidException
  }
  var isFullAcidTable: Boolean = hTable.getParameters.containsKey("transactional_properties") &&
    !hTable.getParameters.get("transactional_properties").equals("insert_only")
  logInfo("Insert Only table: " + !isFullAcidTable)

  val dataSchema = StructType(hTable.getSd.getCols.toList.map(fromHiveColumn).toArray)
  val partitionSchema = StructType(hTable.getPartitionKeys.toList.map(fromHiveColumn).toArray)

  override val schema: StructType = {
    val overlappedPartCols = mutable.Map.empty[String, StructField]
    partitionSchema.foreach { partitionField =>
      if (dataSchema.exists(getColName(_) == getColName(partitionField))) {
        overlappedPartCols += getColName(partitionField) -> partitionField
      }
    }
    StructType(dataSchema.map(f => overlappedPartCols.getOrElse(getColName(f), f)) ++
      partitionSchema.filterNot(f => overlappedPartCols.contains(getColName(f))))
  }

  override def sizeInBytes: Long = {
    val compressionFactor = sqlContext.sparkSession.sessionState.conf.fileCompressionFactor
    (sqlContext.sparkSession.sessionState.conf.defaultSizeInBytes * compressionFactor).toLong
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
    val partitionedColumnSet = hTable.getPartitionKeys.map(_.getName).toSet
    val requiredNonPartitionedColumns = requiredColumns.filter(x => !partitionedColumnSet.contains(x))
    val requiredHiveFields = requiredColumns.map(x => hTable.getAllCols.find(_.getName == x).get)
    val requiredAttributes = requiredHiveFields.map { x =>
      PrettyAttribute(x.getName, getSparkSQLDataType(x))
    }
    val partitionAttributes = hTable.getPartitionKeys.map { x =>
      PrettyAttribute(x.getName, getSparkSQLDataType(x))
    }

    val hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
    val (partitionFilters, otherFilters) = filters.partition { predicate =>
      !predicate.references.isEmpty &&
        predicate.references.toSet.subsetOf(hTable.getPartColNames.toSet)
    }
    val dataFilters = otherFilters.filter(_
      .references.intersect(hTable.getPartColNames).isEmpty
    )
    logInfo(s"total filters : ${filters.size}: " +
      s"dataFilters: ${dataFilters.size} " +
      s"partitionFilters: ${partitionFilters.size}")

    setPushDownFiltersInHadoopConf(hadoopConf, dataFilters)
    setRequiredColumnsInHadoopConf(hadoopConf, requiredNonPartitionedColumns)

    logDebug(s"sarg.pushdown: ${hadoopConf.get("sarg.pushdown")}," +
      s"hive.io.file.readcolumn.names: ${hadoopConf.get("hive.io.file.readcolumn.names")}, " +
      s"hive.io.file.readcolumn.ids: ${hadoopConf.get("hive.io.file.readcolumn.ids")}")

    val acidState = new HiveAcidState(sqlContext.sparkSession, hiveConf, hTable,
      sqlContext.sparkSession.sessionState.conf.defaultSizeInBytes, partitionSchema, isFullAcidTable)

    val hiveReader = new HiveTableReader(
      requiredAttributes,
      partitionAttributes,
      tableDesc,
      sqlContext.sparkSession,
      acidState,
      hadoopConf)
    if (hTable.isPartitioned) {
      val requiredPartitions = getRawPartitions(partitionFilters)
      hiveReader.makeRDDForPartitionedTable(requiredPartitions).asInstanceOf[RDD[Row]]
    } else {
      hiveReader.makeRDDForTable(hTable).asInstanceOf[RDD[Row]]
    }
  }

  private def setRequiredColumnsInHadoopConf(conf: Configuration, requiredColumns: Seq[String]): Unit = {
    val dataCols: Seq[String] = hTable.getCols.map(_.getName)
    val requiredColumnIndexes = requiredColumns.map(a => dataCols.indexOf(a): Integer)
    val (sortedIDs, sortedNames) = requiredColumnIndexes.zip(requiredColumns).sorted.unzip
    conf.set(ColumnProjectionUtils.READ_ALL_COLUMNS, "false")
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, sortedNames.mkString(","))
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, sortedIDs.mkString(","))
  }

  private def setPushDownFiltersInHadoopConf(conf: Configuration, dataFilters: Array[Filter]): Unit = {
    if (isPredicatePushdownEnabled()) {
      OrcFilters.createFilter(dataSchema, dataFilters).foreach { f =>
        def toKryo(obj: com.qubole.shaded.hadoop.hive.ql.io.sarg.SearchArgument): String = {
          val out = new Output(4 * 1024, 10 * 1024 * 1024)
          new Kryo().writeObject(out, obj)
          out.close()
          return Base64.encodeBase64String(out.toBytes)
        }

        logDebug(s"searchArgument: ${f}")
        conf.set("sarg.pushdown", toKryo(f))
        conf.setBoolean(ConfVars.HIVEOPTINDEXFILTER.varname, true)
      }
    }
  }

  private def isPredicatePushdownEnabled(): Boolean = {
    val sqlConf = sqlContext.sparkSession.sessionState.conf
    sqlConf.getConfString("spark.sql.acidDs.enablePredicatePushdown", "true") == "true"
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


  def getRawPartitions(partitionFilters: Array[Filter]): Seq[metadata.Partition] = {
    val prunedPartitions =
      if (sqlContext.sparkSession.sessionState.conf.metastorePartitionPruning &&
        partitionFilters.size > 0) {
        val normalizedFilters = convertFilters(hTable.getTTable, partitionFilters)
        val hive: Hive = Hive.get(hiveConf)
        val hT = hive.getPartitionsByFilter(hTable, normalizedFilters)
        Hive.closeCurrent()
        hT
      } else {
        val hive: Hive = Hive.get(hiveConf)
        val hT = hive.getPartitions(hTable)
        Hive.closeCurrent()
        hT
      }
    logDebug(s"partition count = ${prunedPartitions.size()}")
    prunedPartitions.toSeq
  }
}


object HiveAcidRelation extends Logging {
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
