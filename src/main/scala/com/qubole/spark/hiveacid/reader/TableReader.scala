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

package com.qubole.spark.hiveacid.reader

import com.qubole.spark.hiveacid.{HiveAcidOperation, ReadConf}
import com.qubole.spark.hiveacid.transaction._
import com.qubole.spark.hiveacid.hive.{HiveAcidMetadata, HiveConverter}
import com.qubole.spark.hiveacid.reader.hive.{HiveAcidReader, HiveAcidReaderOptions}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources.Filter

/**
  * Table reader object
  *
  * @param sparkSession - Spark session
  * @param curTxn - Transaction object to acquire locks.
  * @param hiveAcidMetadata - Hive acid table for which read is to be performed.
  */
private[hiveacid] class TableReader(sparkSession: SparkSession,
                                    curTxn: HiveAcidTxn,
                                    hiveAcidMetadata: HiveAcidMetadata) extends Logging {

  def getRdd(requiredColumns: Array[String],
             filters: Array[Filter],
             readConf: ReadConf): RDD[Row] = {


    val rowIdColumnSet = hiveAcidMetadata.rowIdSchema.fields.map(_.name).toSet
    val requiredColumnsWithoutRowId = requiredColumns.filterNot(rowIdColumnSet.contains)
    val partitionColumnNames = hiveAcidMetadata.partitionSchema.fields.map(_.name)
    val partitionedColumnSet = partitionColumnNames.toSet

    // Attributes
    val requiredNonPartitionedColumns = requiredColumnsWithoutRowId.filter(
      x => !partitionedColumnSet.contains(x))

    val requiredAttributes = requiredColumnsWithoutRowId.map {
      x =>
        val field = hiveAcidMetadata.tableSchema.fields.find(_.name == x).get
        PrettyAttribute(field.name, field.dataType)
    }
    val partitionAttributes = hiveAcidMetadata.partitionSchema.fields.map { x =>
      PrettyAttribute(x.name, x.dataType)
    }

    // Filters
    val (partitionFilters, otherFilters) = filters.partition { predicate =>
      !predicate.references.isEmpty &&
        predicate.references.toSet.subsetOf(partitionedColumnSet)
    }
    val dataFilters = otherFilters.filter(_
      .references.intersect(partitionColumnNames).isEmpty
    )

    logDebug(s"total filters : ${filters.length}: " +
      s"dataFilters: ${dataFilters.length} " +
      s"partitionFilters: ${partitionFilters.length}")

    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    logDebug(s"sarg.pushdown: ${hadoopConf.get("sarg.pushdown")}," +
      s"hive.io.file.readcolumn.names: ${hadoopConf.get("hive.io.file.readcolumn.names")}, " +
      s"hive.io.file.readcolumn.ids: ${hadoopConf.get("hive.io.file.readcolumn.ids")}")

    val readerOptions = new ReaderOptions(hadoopConf,
      partitionAttributes,
      requiredAttributes,
      dataFilters,
      requiredNonPartitionedColumns,
      readConf)

    val hiveAcidReaderOptions= HiveAcidReaderOptions.get(hiveAcidMetadata, readConf.includeRowIds)

    val (partitions, partitionList) = HiveAcidReader.getPartitions(hiveAcidMetadata,
      readerOptions,
      partitionFilters)

    // Acquire lock on all the partition and then create snapshot. Every time getRDD is called
    // it creates a new snapshot.
    // NB: partitionList is Seq if partition pruning is not enabled
    curTxn.acquireLocks(hiveAcidMetadata, HiveAcidOperation.READ, partitionList)

    // Create Snapshot !!!
    val curSnapshot = HiveAcidTxn.createSnapshot(curTxn, hiveAcidMetadata)

    val reader = new HiveAcidReader(
      sparkSession,
      readerOptions,
      hiveAcidReaderOptions,
      curSnapshot.validWriteIdList)

    val rdd = if (hiveAcidMetadata.isPartitioned) {
      reader.makeRDDForPartitionedTable(hiveAcidMetadata, partitions)
    } else {
      reader.makeRDDForTable(hiveAcidMetadata)
    }


    rdd.asInstanceOf[RDD[Row]]
  }
}
