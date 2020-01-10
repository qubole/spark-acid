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

package com.qubole.spark.hiveacid.reader.hive

import com.qubole.shaded.hadoop.hive.ql.plan.TableDesc
import com.qubole.spark.hiveacid.hive.HiveAcidMetadata
import org.apache.spark.sql.types.StructType

private[reader] class HiveAcidReaderOptions(val tableDesc: TableDesc,
                                            val rowIdSchema: Option[StructType],
                                            val isFullAcidTable: Boolean)

private[reader] object HiveAcidReaderOptions {
  def get(hiveAcidMetadata: HiveAcidMetadata, includeRowIds: Boolean): HiveAcidReaderOptions = {
    val rowIdSchema = if (includeRowIds) {
      Option(hiveAcidMetadata.rowIdSchema)
    } else {
      None
    }
    new HiveAcidReaderOptions(hiveAcidMetadata.tableDesc, rowIdSchema, hiveAcidMetadata.isFullAcidTable)
  }
}
