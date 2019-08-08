/*
 * Copyright 2019 Qubole, Inc.  All rights reserved.
 *
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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.qubole.shaded.hadoop.hive.conf.HiveConf
import com.qubole.shaded.hadoop.hive.conf.HiveConf.ConfVars
import com.qubole.shaded.hadoop.hive.ql.metadata
import com.qubole.shaded.hadoop.hive.ql.metadata.Hive
import com.qubole.spark.datasources.hiveacid.orc.OrcFilters
import com.qubole.spark.datasources.hiveacid.rdd.HiveTableReader
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.PrettyAttribute
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConversions._

class HiveAcidRelation(var sqlContext: SQLContext,
                       parameters: Map[String, String])
    extends BaseRelation
    with PrunedFilteredScan
    with Logging {

  private val fullyQualifiedTableName: String = parameters.getOrElse("table", {
    throw HiveAcidErrors.tableNotSpecifiedException
  })
  val hiveConf: HiveConf = HiveAcidRelation.createHiveConf(sqlContext.sparkContext)
  val hiveAcidTable: HiveAcidTable = HiveAcidTable.fromTableName(fullyQualifiedTableName,
    hiveConf)

  override val schema: StructType = hiveAcidTable.schema

  override def sizeInBytes: Long = {
    val compressionFactor = sqlContext.sparkSession.sessionState.conf.fileCompressionFactor
    (sqlContext.sparkSession.sessionState.conf.defaultSizeInBytes * compressionFactor).toLong
  }

  override val needConversion: Boolean = false

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val tableDesc = hiveAcidTable.tableDesc
    val hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
    val partitionColumnNames = hiveAcidTable.partitionSchema.fields.map(_.name)
    val partitionedColumnSet = partitionColumnNames.toSet
    val requiredNonPartitionedColumns = requiredColumns.filter(
      x => !partitionedColumnSet.contains(x))
    val requiredAttributes = requiredColumns.map {
      x =>
        val field = hiveAcidTable.schema.fields.find(_.name == x).get
        PrettyAttribute(field.name, field.dataType)
    }
    val partitionAttributes = hiveAcidTable.partitionSchema.fields.map { x =>
      PrettyAttribute(x.name, x.dataType)
    }

    val (partitionFilters, otherFilters) = filters.partition { predicate =>
      !predicate.references.isEmpty &&
        predicate.references.toSet.subsetOf(partitionedColumnSet)
    }
    val dataFilters = otherFilters.filter(_
      .references.intersect(partitionColumnNames).isEmpty
    )
    logInfo(s"total filters : ${filters.size}: " +
      s"dataFilters: ${dataFilters.size} " +
      s"partitionFilters: ${partitionFilters.size}")

    setPushDownFiltersInHadoopConf(hadoopConf, dataFilters)
    setRequiredColumnsInHadoopConf(hadoopConf, requiredNonPartitionedColumns)

    logDebug(s"sarg.pushdown: ${hadoopConf.get("sarg.pushdown")}," +
      s"hive.io.file.readcolumn.names: ${hadoopConf.get("hive.io.file.readcolumn.names")}, " +
      s"hive.io.file.readcolumn.ids: ${hadoopConf.get("hive.io.file.readcolumn.ids")}")

    val acidState = new HiveAcidState(sqlContext.sparkSession, hiveConf, hiveAcidTable,
      sqlContext.sparkSession.sessionState.conf.defaultSizeInBytes)

    val hiveReader = new HiveTableReader(
      requiredAttributes,
      partitionAttributes,
      tableDesc,
      sqlContext.sparkSession,
      acidState,
      hadoopConf)
    if (hiveAcidTable.isPartitioned) {
      val requiredPartitions = getRawPartitions(partitionFilters)
      hiveReader.makeRDDForPartitionedTable(requiredPartitions).asInstanceOf[RDD[Row]]
    } else {
      hiveReader.makeRDDForTable(hiveAcidTable.hTable).asInstanceOf[RDD[Row]]
    }
  }

  private def setRequiredColumnsInHadoopConf(conf: Configuration,
                                             requiredColumns: Seq[String]): Unit = {
    val dataCols: Seq[String] = hiveAcidTable.dataSchema.fields.map(_.name)
    val requiredColumnIndexes = requiredColumns.map(a => dataCols.indexOf(a): Integer)
    val (sortedIDs, sortedNames) = requiredColumnIndexes.zip(requiredColumns).sorted.unzip
    conf.set(ColumnProjectionUtils.READ_ALL_COLUMNS, "false")
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, sortedNames.mkString(","))
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, sortedIDs.mkString(","))
  }

  private def setPushDownFiltersInHadoopConf(conf: Configuration,
                                             dataFilters: Array[Filter]): Unit = {
    if (isPredicatePushdownEnabled()) {
      OrcFilters.createFilter(hiveAcidTable.dataSchema, dataFilters).foreach { f =>
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


  private def convertFilters(filters: Seq[Filter]): String = {
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
        val normalizedFilters = convertFilters(partitionFilters)
        val hive: Hive = Hive.get(hiveConf)
        val hT = hive.getPartitionsByFilter(hiveAcidTable.hTable, normalizedFilters)
        Hive.closeCurrent()
        hT
      } else {
        val hive: Hive = Hive.get(hiveConf)
        val hT = hive.getPartitions(hiveAcidTable.hTable)
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
