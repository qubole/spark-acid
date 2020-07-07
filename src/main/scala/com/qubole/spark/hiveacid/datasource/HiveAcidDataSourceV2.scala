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

package com.qubole.spark.hiveacid.datasource

import java.util.{ArrayList, List, Map}
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.internal.Logging
import com.qubole.spark.hiveacid.HiveAcidDataSourceV2Reader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.v2.{ReadSupport,DataSourceOptions,DataSourceV2}

class HiveAcidDataSourceV2 extends DataSourceV2 with ReadSupport with Logging {
  override def  createReader (options: DataSourceOptions) : DataSourceReader = {
    logInfo("Using Datasource V2 for table" + options.tableName.get)
    new HiveAcidDataSourceV2Reader(options.asMap,
                                  SparkSession.getActiveSession.orNull,
                                  options.databaseName.get, options.tableName.get)
  }

  def keyPrefix() : String = {
    "HiveAcidV2"
  }
}
