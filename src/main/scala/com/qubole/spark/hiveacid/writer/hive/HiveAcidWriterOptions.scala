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

package com.qubole.spark.hiveacid.writer.hive

import com.qubole.shaded.hadoop.hive.ql.plan.FileSinkDesc
import com.qubole.spark.hiveacid.HiveAcidOperation
import com.qubole.spark.hiveacid.hive.HiveAcidMetadata
import com.qubole.spark.hiveacid.writer.WriterOptions
import org.apache.hadoop.fs.Path

private[writer] class HiveAcidWriterOptions(val rootPath: String,
                                            fileSinkDesc: FileSinkDesc) extends Serializable {
  lazy val getFileSinkDesc: FileSinkDesc = {
    fileSinkDesc.setDirName(new Path(rootPath))
    fileSinkDesc
  }
}

private[writer] object HiveAcidWriterOptions {
  def get(hiveAcidMetadata: HiveAcidMetadata,
                            options: WriterOptions): HiveAcidWriterOptions = {
    lazy val fileSinkDescriptor: FileSinkDesc = {
      val fileSinkDesc: FileSinkDesc = new FileSinkDesc()
      fileSinkDesc.setTableInfo(hiveAcidMetadata.tableDesc)
      fileSinkDesc.setTableWriteId(options.currentWriteId)
      if (options.operationType == HiveAcidOperation.INSERT_OVERWRITE) {
        fileSinkDesc.setInsertOverwrite(true)
      }
      fileSinkDesc
    }
    new HiveAcidWriterOptions(rootPath = hiveAcidMetadata.rootPath.toUri.toString,
      fileSinkDesc = fileSinkDescriptor)
  }

}
