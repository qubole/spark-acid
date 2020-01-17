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
package com.qubole.spark

import org.apache.spark.sql._

package object hiveacid {
  implicit class HiveAcidDataFrameReader(reader: DataFrameReader) {
    def hiveacid(table: String, options: Map[String, String] = Map.empty): DataFrame = {
      reader.format("HiveAcid").option("table", table)
        .options(options).load()
    }
  }

  implicit class HiveAcidDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def hiveacid(table: String, saveMode: String, options: Map[String, String] = Map.empty): Unit = {
      writer.format("HiveAcid").option("table", table)
        .options(options).mode(saveMode).save()
    }
  }
}
