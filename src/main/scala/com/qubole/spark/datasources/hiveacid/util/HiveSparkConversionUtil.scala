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

package com.qubole.spark.datasources.hiveacid.util

import java.util.Locale

import com.qubole.shaded.hadoop.hive.conf.HiveConf
import com.qubole.shaded.hadoop.hive.metastore.api.FieldSchema
import com.qubole.spark.datasources.hiveacid.HiveAcidRelation.logDebug
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

object HiveSparkConversionUtil extends Logging {
  def hiveColumnToSparkColumn(hc: FieldSchema): StructField = {
    val columnType = getSparkSQLDataType(hc.getType)
    val metadata = if (hc.getType != columnType.catalogString) {
      new MetadataBuilder().putString(HIVE_TYPE_STRING, hc.getType).build()
    } else {
      Metadata.empty
    }

    val field = StructField(
      name = hc.getName,
      dataType = columnType,
      nullable = true,
      metadata = metadata)
    Option(hc.getComment).map(field.withComment).getOrElse(field)
  }

  def getSparkSQLDataType(dataType: String): DataType = {
    try {
      CatalystSqlParser.parseDataType(dataType)
    } catch {
      case e: ParseException =>
        throw new SparkException("Cannot recognize hive type string: " + dataType, e)
    }
  }

  def createHiveConf(sparkContext: SparkContext): HiveConf = {
    val hiveConf = new HiveConf()
    (sparkContext.hadoopConfiguration.iterator().map(kv => kv.getKey -> kv.getValue)
      ++ sparkContext.getConf.getAll.toMap).foreach { case (k, v) =>
      logDebug(
        s"""
           |Applying Hadoop/Hive/Spark and extra properties to Hive Conf:
           |$k=${if (k.toLowerCase(Locale.ROOT).contains("password")) "xxx" else v}
         """.stripMargin)
      hiveConf.set(k, v)
    }
    hiveConf
  }

}
