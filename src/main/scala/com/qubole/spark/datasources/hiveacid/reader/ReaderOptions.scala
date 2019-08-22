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

package com.qubole.spark.datasources.hiveacid.reader

import com.qubole.shaded.hadoop.hive.ql.plan.TableDesc
import com.qubole.spark.datasources.hiveacid.HiveAcidMetadata
import com.qubole.spark.datasources.hiveacid.ReadConf
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 * Reader options which will be serialized and sent to each executor
 */
private[reader] class ReaderOptions(val hadoopConf: Configuration,
                                    val partitionAttributes: Seq[Attribute],
                                    val requiredAttributes: Seq[Attribute],
                                    val dataFilters: Array[Filter],
                                    val requiredNonPartitionedColumns: Array[String],
                                    val readConf: ReadConf) extends Serializable

private[reader] class Hive3ReaderOptions(val tableDesc: TableDesc,
                                         val rowIdSchema: Option[StructType])

private[reader] object Hive3ReaderOptions {
  def get(hiveAcidMetadata: HiveAcidMetadata, includeRowIds: Boolean): Hive3ReaderOptions = {
    val rowIdSchema = if (includeRowIds) {
      Option(hiveAcidMetadata.rowIdSchema)
    } else {
      None
    }
    new Hive3ReaderOptions(hiveAcidMetadata.tableDesc, rowIdSchema)
  }
}
