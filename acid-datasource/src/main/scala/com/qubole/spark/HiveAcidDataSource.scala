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

import java.util.Locale

import com.qubole.shaded.hive.conf.HiveConf
import com.qubole.shaded.hive.metastore.api.FieldSchema
import com.qubole.shaded.hive.ql.exec.Utilities
import com.qubole.shaded.hive.ql.metadata.HiveUtils
import com.qubole.shaded.hive.ql.plan.TableDesc
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, _}

import scala.collection.JavaConversions._

class HiveAcidDataSource
  extends RelationProvider
    with DataSourceRegister
    with Logging {

  override def createRelation(
   sqlContext: SQLContext,
   parameters: Map[String, String]): BaseRelation = {
    new HiveAcidRelation(
      sqlContext,
      parameters
    )
  }

  override def shortName(): String = {
    HiveAcidUtils.NAME
  }
}

object HiveAcidDataSource extends Logging {
  //TODO Somani: This doesn't seem to be used?
//  def initializeLocalJobConfFunc(path: String, tableDesc: TableDesc,
//                                 schemaColNames: String, schemaColTypes: String)(jobConf: JobConf) {
//    FileInputFormat.setInputPaths(jobConf, Seq[Path](new Path(path)): _*)
//    if (tableDesc != null) {
//      HiveAcidDataSource.configureJobPropertiesForStorageHandler(tableDesc, jobConf, true)
//      Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
//    }
//    val bufferSize = System.getProperty("spark.buffer.size", "65536")
//    jobConf.set("io.file.buffer.size", bufferSize)
//    jobConf.set("schema.evolution.columns", schemaColNames)
//    jobConf.set("schema.evolution.columns.types", schemaColTypes)
//    jobConf.setBoolean("hive.transactional.table.scan", true)
//  }

  def configureJobPropertiesForStorageHandler(
                                               tableDesc: TableDesc, conf: Configuration, input: Boolean) {
    val property = tableDesc.getProperties.getProperty(
      com.qubole.shaded.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE
    )
    val storageHandler =
      HiveUtils.getStorageHandler(conf, property)
    if (storageHandler != null) {
      val jobProperties = new java.util.LinkedHashMap[String, String]
      if (input) {
        storageHandler.configureInputJobProperties(tableDesc, jobProperties)
      } else {
        storageHandler.configureOutputJobProperties(tableDesc, jobProperties)
      }
      if (!jobProperties.isEmpty) {
        tableDesc.setJobProperties(jobProperties)
      }
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

  val agentName: String = "HiveAcidDataSource"

}
