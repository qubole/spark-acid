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

import org.apache.hadoop.conf.Configuration
import com.qubole.shaded.hive.conf.HiveConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, FileIndex, FileStatusCache, InMemoryFileIndex, PartitionDirectory, PartitionPath, PartitionSpec, PrunedInMemoryFileIndex}
import com.qubole.shaded.hive.metastore.HiveMetaStoreClient
import com.qubole.shaded.hive.metastore.api.MetaException
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, GenericInternalRow, Literal}
import com.qubole.shaded.hive.metastore.api.MetaException
import org.apache.thrift.TException
import com.qubole.shaded.hive.metastore.api.Table
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConversions._

class HiveAcidFileIndex (sparkSession: SparkSession,
                         val table: Table,
                         val sizeInBytes: Long,
                         val client: HiveMetaStoreClient,
                         val pSchema: StructType) extends FileIndex {


  var _txnId: Long = -1
  override def rootPaths: Seq[Path] = Seq(new Path(table.getSd.getLocation)) //.map(new Path(_)).toSeq

  private val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)

  override def refresh(): Unit = fileStatusCache.invalidateAll()

  override def partitionSchema: StructType = pSchema

  override def inputFiles: Array[String] = new InMemoryFileIndex(
    sparkSession, rootPaths, Option(table.getSd.getSerdeInfo.getParameters)
      .map(_.toMap).orNull, userSpecifiedSchema = None).inputFiles

  override def listFiles(
                          partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    if (_txnId == -1) {
      _txnId = client.openTxn("HiveAcidDataSource")
    }
    // Somani: Need to take care partitioning here
    var validTxns = client.getValidTxns(_txnId)
    var validWriteIds = client.getValidWriteIds(Seq(table.getDbName + "." + table.getTableName), client.getValidTxns(_txnId).writeToString())
    new InMemoryFileIndex(
      sparkSession, rootPaths, Option(table.getSd.getSerdeInfo.getParameters)
        .map(_.toMap).orNull, userSpecifiedSchema = None).listFiles(Nil, dataFilters)
  }

  def close(): Unit = {
    if (_txnId != -1) {
      client.commitTxn(_txnId)
      _txnId = -1
    }
  }
}
