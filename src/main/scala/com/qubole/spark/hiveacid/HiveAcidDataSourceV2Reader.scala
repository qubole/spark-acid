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

package com.qubole.spark.hiveacid

import java.lang.String.format
import java.io.IOException
import java.util.{ArrayList, List, Map}

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import com.qubole.spark.hiveacid.hive.{HiveAcidMetadata, HiveConverter}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import com.qubole.spark.hiveacid.transaction.HiveAcidTxn
import com.qubole.spark.hiveacid.util.{SerializableConfiguration, Util}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader._
import com.qubole.spark.hiveacid.reader.v2.HiveAcidInputPartitionV2
import com.qubole.spark.hiveacid.reader.TableReader
import com.qubole.spark.hiveacid.reader.hive.HiveAcidSearchArgument
import com.qubole.spark.hiveacid.reader.hive.HiveAcidSearchArgument.{buildTree, castLiteralValue, getPredicateLeafType, isSearchableType, quoteAttributeNameIfNeeded}

/**
  * Data source V2 implementation for HiveACID
*/
class HiveAcidDataSourceV2Reader
  extends DataSourceV2 with DataSourceReader with SupportsScanColumnarBatch
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters  with Logging {

  def this(options: java.util.Map[String, String],
           sparkSession : SparkSession,
           dbName : String,
           tblName : String) {
    this()
    this.options = options
    this.sparkSession = sparkSession
    if (dbName != null) {
      hiveAcidMetadata = HiveAcidMetadata.fromSparkSession(sparkSession, dbName + "." + tblName)
    } else {
      // If db name is null, default db is chosen.
      hiveAcidMetadata = HiveAcidMetadata.fromSparkSession(sparkSession, tblName)
    }

    // This is a hack to prevent the following situation:
    // Spark(v 2.4.0) creates one instance of DataSourceReader to call readSchema()
    // and then a new instance of DataSourceReader to call pushFilters(),
    // planBatchInputPartitions() etc. Since it uses different DataSourceReader instances,
    // and reads schema in former instance, schema remains null in the latter instance
    // (which causes problems for other methods). More discussion:
    // http://apache-spark-user-list.1001560.n3.nabble.com/DataSourceV2-APIs-creating-multiple-instances-of-DataSourceReader-and-hence-not-preserving-the-state-tc33646.html
    // Also a null check on schema is already there in readSchema() to prevent initialization
    // more than once just in case.
    readSchema
  }

  private var options: java.util.Map[String, String] = null
  private var sparkSession : SparkSession = null

  //The pruned schema
  private var schema: StructType = null

  private var pushedFilterArray : Array[Filter] = null

  private var hiveAcidMetadata: HiveAcidMetadata = _

  override def readSchema: StructType = {
    if (schema == null) {
      schema = hiveAcidMetadata.tableSchema
    }
    schema
  }

  override def planBatchInputPartitions() : java.util.List[InputPartition[ColumnarBatch]] = {
    val factories = new java.util.ArrayList[InputPartition[ColumnarBatch]]
    inTxn {
      txn: HiveAcidTxn => {
        import scala.collection.JavaConversions._
        val reader = new TableReader(sparkSession, txn, hiveAcidMetadata)
        val hiveReader = reader.getPartitionsV2(schema.fieldNames,
          pushedFilterArray, new SparkAcidConf(sparkSession, options.toMap))
        factories.addAll(hiveReader)
      }
    }
    factories
  }

  private def inTxn(f: HiveAcidTxn =>  Unit): Unit = {
    new HiveTxnWrapper(sparkSession).inTxn(f)
  }

  override def pushFilters (filters: Array[Filter]): Array[Filter] = {
    this.pushedFilterArray = HiveAcidSearchArgument.
      getSupportedFilters(hiveAcidMetadata.tableSchema, filters.toSeq).toArray
    filters.filterNot(filter => this.pushedFilterArray.contains(filter))
  }

  override def pushedFilters(): Array[Filter] = this.pushedFilterArray

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.schema = requiredSchema
  }
}