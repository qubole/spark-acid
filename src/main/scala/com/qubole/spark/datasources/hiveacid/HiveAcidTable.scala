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

package com.qubole.spark.datasources.hiveacid

import scala.collection.Map

import com.qubole.spark.datasources.hiveacid.reader.TableReader
import com.qubole.spark.datasources.hiveacid.transaction.HiveAcidTxnManager
import com.qubole.spark.datasources.hiveacid.util.HiveConverter
import com.qubole.spark.datasources.hiveacid.writer.TableWriter

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.sources.Filter


/**
 */
/**
  * Represents a hive acid table and exposes API to perform operations on top of it
  * @param sparkSession - spark session object
  * @param parameters - additional parameters
  * @param hiveAcidMetadata - metadata object
  */
class HiveAcidTable(sparkSession: SparkSession,
                    parameters: Map[String, String],
                    hiveAcidMetadata: HiveAcidMetadata) extends Logging {

  private val hiveConf = HiveConverter.getHiveConf(sparkSession.sparkContext)

  // txnManager is kept outside of TableReader/TableWriter as in future we might have to club
  // multiple Reader/Writer operations in single transaction
  private val txnManager = new HiveAcidTxnManager(sparkSession, hiveConf)

  /**
    * Return an RDD on top of Hive ACID table
    * @param requiredColumns - columns needed
    * @param filters - filters that can be pushed down to file format
    * @param readConf - read conf
    * @return
    */
  def getRdd(requiredColumns: Array[String],
             filters: Array[Filter],
             readConf: ReadConf): RDD[Row] = {
    val tableReader = new TableReader(sparkSession, txnManager, hiveAcidMetadata)
    tableReader.getRdd(requiredColumns, filters, readConf)
  }

  /**
    * Appends a given dataframe df into the hive acid table
    * @param df - dataframe to insert
    */
  def insertInto(df: DataFrame): Unit = {
    val tableWriter = new TableWriter(sparkSession, txnManager, hiveAcidMetadata)
    tableWriter.write(HiveAcidOperation.INSERT_INTO, df)
  }

  /**
    * Overwrites a given dataframe df onto the hive acid table
    * @param df - dataframe to insert
    */
  def insertOverwrite(df: DataFrame): Unit = {
    val tableWriter = new TableWriter(sparkSession, txnManager, hiveAcidMetadata)
    tableWriter.write(HiveAcidOperation.INSERT_OVERWRITE, df)
  }

}

object HiveAcidTable {
  def fromSparkSession(sparkSession: SparkSession,
                       parameters: Map[String, String],
                       fullyQualifiedTableName: String): HiveAcidTable = {

    val hiveAcidMetadata = HiveAcidMetadata.fromSparkSession(sparkSession, fullyQualifiedTableName)
    new HiveAcidTable(sparkSession, parameters, hiveAcidMetadata)
  }
}
