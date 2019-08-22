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

package com.qubole.spark.datasources.hiveacid.writer

import com.qubole.shaded.hadoop.hive.ql.plan.FileSinkDesc
import com.qubole.spark.datasources.hiveacid.{HiveAcidMetadata, HiveAcidOperation}

import org.apache.spark.sql.catalyst.InternalRow

private[writer] trait Writer {
  def process(row: InternalRow): Unit
  def close(): Unit
}

private[writer] object Writer {
  def getHive3Writer(hiveAcidMetadata: HiveAcidMetadata,
                     options: WriterOptions): Writer = {

    lazy val fileSinkDescriptor: FileSinkDesc = {
      val fileSinkDesc = new FileSinkDesc()
      fileSinkDesc.setDirName(hiveAcidMetadata.rootPath)
      fileSinkDesc.setTableInfo(hiveAcidMetadata.tableDesc)
      fileSinkDesc.setTableWriteId(options.currentWriteId)
      if (options.operationType == HiveAcidOperation.INSERT_OVERWRITE) {
        fileSinkDesc.setInsertOverwrite(true)
      }
      fileSinkDesc
    }

    val hive3Options = new Hive3WriterOptions(rootPath = hiveAcidMetadata.rootPath.toUri.toString,
                                              fileSinkConf = fileSinkDescriptor)

    if (hiveAcidMetadata.isFullAcidTable) {
      new Hive3FullAcidWriter(options, hive3Options)
    } else {
      new Hive3InsertOnlyWriter(options, hive3Options)
    }
  }
}
