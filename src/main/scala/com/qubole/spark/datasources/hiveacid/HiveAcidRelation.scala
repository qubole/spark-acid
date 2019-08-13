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
import com.qubole.shaded.hadoop.hive.ql.io.RecordIdentifier
import com.qubole.shaded.hadoop.hive.ql.metadata
import com.qubole.shaded.hadoop.hive.ql.metadata.Hive
import com.qubole.spark.datasources.hiveacid.orc.OrcFilters
import com.qubole.spark.datasources.hiveacid.rdd.HiveTableReader
import com.qubole.spark.datasources.hiveacid.util.HiveSparkConversionUtil
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
  private val includeRowIds: Boolean = parameters.getOrElse("includeRowIds", "false").toBoolean

  val hiveConf: HiveConf = HiveSparkConversionUtil.createHiveConf(sqlContext.sparkContext)
  val hiveAcidTable: HiveAcidTable = HiveAcidTable.fromTableName(fullyQualifiedTableName,
    hiveConf)

  override val schema: StructType = if (includeRowIds) {
    hiveAcidTable.tableSchemaWithRowId
  } else {
    hiveAcidTable.tableSchema
  }

  override def sizeInBytes: Long = {
    val compressionFactor = sqlContext.sparkSession.sessionState.conf.fileCompressionFactor
    (sqlContext.sparkSession.sessionState.conf.defaultSizeInBytes * compressionFactor).toLong
  }

  override val needConversion: Boolean = false

  override def buildScan(requiredColumnsAll: Array[String], filters: Array[Filter]): RDD[Row] = {
    val requiredColumns = requiredColumnsAll.filterNot(hiveAcidTable.rowIdColumnSet.contains)
    val tableDesc = hiveAcidTable.tableDesc
    val hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
    val partitionColumnNames = hiveAcidTable.partitionSchema.fields.map(_.name)
    val partitionedColumnSet = partitionColumnNames.toSet
    val requiredNonPartitionedColumns = requiredColumns.filter(
      x => !partitionedColumnSet.contains(x))
    val requiredAttributes = requiredColumns.map {
      x =>
        val field = hiveAcidTable.tableSchema.fields.find(_.name == x).get
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
      includeRowIds,
      hadoopConf)
    if (hiveAcidTable.isPartitioned) {
      val requiredPartitions = hiveAcidTable.getRawPartitions(
        HiveSparkConversionUtil.sparkToHiveFilters(partitionFilters),
        sqlContext.sparkSession.sessionState.conf.metastorePartitionPruning,
        hiveConf)
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

}


object HiveAcidRelation extends Logging {
}
