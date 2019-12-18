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

package com.qubole.spark.hiveacid

import org.joda.time.DateTime
import scala.collection.mutable.{ListBuffer, HashMap}

/*
 *
 *
 */
class Table (
  private val dbName: String,
  private val tName: String,
  private val extraColMap: Map[String, String],
  private val tblProp: String,
  val isPartitioned: Boolean = false) {

    private var colMap = Map("key" -> "int") ++ extraColMap
    private var colMapWithPartitionedCols = if (isPartitioned) {
        Map("ptnCol" -> "int") ++ colMap
      } else {
        colMap
      }

  // NB Add date column as well apparently always in the end
  private def getRow(key: Int): String = colMap.map( x => {
    x._2 match {
      case "date" => s"'${(new DateTime(((key * 1000L) + 151502791900L))).toString}'"
      case _ => {
        x._1 match {
          case "ptnCol" => (key % 3).toString
          case _ =>        key.toString
        }
    }
  }}).mkString(", ") + {if (isPartitioned) s", '${(new DateTime(((key * 1000L) + 151502791900L))).toString}'" else ""}

  private def getColDefString = colMap.map(x => x._1 + " " + x._2).mkString(",")

  // FIXME: Add ptn_col column of partitioned table in order by clause
  // private def sparkOrderBy: String = sparkOrderBy(sparkTname)
  // private def hiveOrderBy: String = hiveOrderBy(tName)
  private def sparkOrderBy(aliasedTable: String): String =
    colMapWithPartitionedCols.map(x => s"${aliasedTable}.${x._1}").mkString(", ")
  private def hiveOrderBy(aliasedTable: String): String =
    colMapWithPartitionedCols.map(x => s"$aliasedTable.${x._1}").mkString(", ")
  private def getCols = colMapWithPartitionedCols.map(x => x._1).mkString(", ")

  def getColMap = colMapWithPartitionedCols

  def hiveTname = s"$dbName.$tName"
  def hiveTname1 = s"$tName"
  def sparkTname = s"${dbName}.spark_${tName}"

  def hiveCreate = s"CREATE TABLE ${hiveTname} (${getColDefString}) ${tblProp}"
  def hiveSelect = s"SELECT * FROM ${hiveTname} t1 ORDER BY ${hiveOrderBy("t1")}"
  def hiveSelectWithPred(pred: String) =
    s"SELECT * FROM ${hiveTname} t1 where ${pred} ORDER BY ${hiveOrderBy("t1")}"
  def hiveSelectWithProj = s"SELECT intCol FROM ${hiveTname} ORDER BY intCol"
  def hiveDrop = s"DROP TABLE IF EXISTS ${hiveTname}"

  def sparkCreate = s"CREATE TABLE ${sparkTname} USING HiveAcid OPTIONS('table' '${hiveTname}')"
  def sparkSelect = s"SELECT * FROM ${sparkTname} t1 ORDER BY ${sparkOrderBy("t1")}"
  def sparkSelectWithPred(pred: String) =
    s"SELECT * FROM ${sparkTname} t1 where ${pred} ORDER BY ${sparkOrderBy("t1")}"
  def sparkSelectWithProj = s"SELECT intCol FROM ${sparkTname} ORDER BY intCol"
  def sparkDFProj = "intCol"
  def sparkDrop = s"DROP TABLE IF EXISTS ${sparkTname}"


  private def insertHiveTableKeyRange(startKey: Int, endKey: Int, operation: String): String =
    s"INSERT ${operation} TABLE ${hiveTname} " + (startKey to endKey).map { key => s" select ${getRow(key)} " }.mkString(" UNION ALL ")
  private def insertSparkTableKeyRange(startKey: Int, endKey: Int, operation: String): String =
    s"INSERT ${operation} TABLE ${sparkTname} " + (startKey to endKey).map { key => s" select ${getRow(key)} " }.mkString(" UNION ALL ")

  def insertIntoHiveTableKeyRange(startKey: Int, endKey: Int): String =
    insertHiveTableKeyRange(startKey, endKey, "INTO")
  def insertIntoSparkTableKeyRange(startKey: Int, endKey: Int): String =
    insertSparkTableKeyRange(startKey, endKey, "INTO")
  def insertOverwriteHiveTableKeyRange(startKey: Int, endKey: Int): String =
    insertHiveTableKeyRange(startKey, endKey, "OVERWRITE")
  def insertOverwriteSparkTableKeyRange(startKey: Int, endKey: Int): String =
    insertSparkTableKeyRange(startKey, endKey, "OVERWRITE")

  def insertIntoHiveTableKey(key: Int): String =
    s"INSERT INTO ${hiveTname} (${getCols}) VALUES (${getRow(key)})"
  def deleteFromHiveTableKey(key: Int): String =
    s"DELETE FROM ${hiveTname} where key = ${key}"
  def updateInHiveTableKey(key: Int): String =
    s"UPDATE ${hiveTname} set intCol = intCol * 10 where key = ${key}"

  def updateByMergeHiveTable =
    s" merge into ${hiveTname} t using (select distinct ${getCols} from ${hiveTname}) s on s.key=t.key " +
      s" when matched and s.key%2=0 then update set intCol=s.intCol * 10 " +
      s" when matched and s.key%2=1 then delete " +
      s" when not matched then insert values(${getRow(1000)})"

  def disableCompaction = s"ALTER TABLE ${hiveTname} SET TBLPROPERTIES ('NO_AUTO_COMPACTION' = 'true')"
  def disableCleanup = s"ALTER TABLE ${hiveTname} SET TBLPROPERTIES ('NO_CLEANUP' = 'true')"
  def minorCompaction = s"ALTER TABLE ${hiveTname} COMPACT 'minor'"
  def majorCompaction = s"ALTER TABLE ${hiveTname} COMPACT 'major'"

  def minorPartitionCompaction(ptnid: Int): String = {
      s"ALTER TABLE ${hiveTname} PARTITION(ptnCol=${ptnid}) COMPACT 'minor'"
  }

  def majorPartitionCompaction(ptnid: Int): String = {
      s"ALTER TABLE ${hiveTname} PARTITION(ptnCol=${ptnid}) COMPACT 'major'"
  }

  def alterToTransactionalInsertOnlyTable =
    s"ALTER TABLE ${hiveTname} SET TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')"
  def alterToTransactionalFullAcidTable =
    s"ALTER TABLE ${hiveTname} SET TBLPROPERTIES ('transactional'='true', 'transactional_properties'='default')"
}

object Table {

  // Create table string builder
  // 1st param
  private val partitionedStr = "PARTITIONED BY (ptnCol int) "
  // 2nd param
  private val clusteredStr = "CLUSTERED BY(key) INTO 3 BUCKETS "
  // 3rd param
  private val orcStr = "STORED AS ORC "
  private val parquetStr = "STORED AS PARQUET "
  private val textStr = "STORED AS TEXTFILE "
  private val avroStr = "STORED AS AVRO "
  // 4th param
  private val fullAcidStr = " TBLPROPERTIES ('transactional'='true', 'transactional_properties'='default') "
  private val insertOnlyStr = "  TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only') "

  val orcTable = orcStr
  val orcPartitionedTable = partitionedStr + orcStr
  val orcBucketedPartitionedTable = partitionedStr + clusteredStr + orcStr
  val orcBucketedTable = clusteredStr + orcStr

  val orcFullACIDTable = orcStr + fullAcidStr
  val orcPartitionedFullACIDTable = partitionedStr + orcStr + fullAcidStr
  val orcBucketedPartitionedFullACIDTable = partitionedStr + clusteredStr + orcStr + fullAcidStr
  val orcBucketedFullACIDTable = clusteredStr + orcStr + fullAcidStr

  val parquetFullACIDTable = parquetStr + fullAcidStr
  val parquetPartitionedFullACIDTable = partitionedStr + parquetStr + fullAcidStr
  val parquetBucketedPartitionedFullACIDTable = partitionedStr + clusteredStr + parquetStr + fullAcidStr
  val parquetBucketedFullACIDTable = clusteredStr + parquetStr + fullAcidStr

  val textFullACIDTable = textStr + fullAcidStr
  val textPartitionedFullACIDTable = partitionedStr + textStr + fullAcidStr
  val textBucketedPartitionedFullACIDTable = partitionedStr + clusteredStr + textStr + fullAcidStr
  val textBucketedFullACIDTable = clusteredStr + textStr + fullAcidStr

  val avroFullACIDTable = avroStr + fullAcidStr
  val avroPartitionedFullACIDTable = partitionedStr + avroStr + fullAcidStr
  val avroBucketedPartitionedFullACIDTable = partitionedStr + clusteredStr + avroStr + fullAcidStr
  val avroBucketedFullACIDTable = clusteredStr + avroStr + fullAcidStr

  val orcInsertOnlyTable = orcStr + insertOnlyStr
  val orcPartitionedInsertOnlyTable = partitionedStr + orcStr + insertOnlyStr
  val orcBucketedPartitionedInsertOnlyTable = partitionedStr + clusteredStr + orcStr + insertOnlyStr
  val orcBucketedInsertOnlyTable = clusteredStr + orcStr + insertOnlyStr

  val parquetInsertOnlyTable = parquetStr + insertOnlyStr
  val parquetPartitionedInsertOnlyTable = partitionedStr + parquetStr + insertOnlyStr
  val parquetBucketedPartitionedInsertOnlyTable = partitionedStr + clusteredStr + parquetStr + insertOnlyStr
  val parquetBucketedInsertOnlyTable = clusteredStr + parquetStr + insertOnlyStr

  val textInsertOnlyTable = textStr + insertOnlyStr
  val textPartitionedInsertOnlyTable = partitionedStr + textStr + insertOnlyStr
  val textBucketedPartitionedInsertOnlyTable = partitionedStr + clusteredStr + textStr + insertOnlyStr
  val textBucketedInsertOnlyTable = clusteredStr + textStr + insertOnlyStr

  val avroInsertOnlyTable = avroStr + insertOnlyStr
  val avroPartitionedInsertOnlyTable = partitionedStr + avroStr + insertOnlyStr
  val avroBucketedPartitionedInsertOnlyTable = partitionedStr + clusteredStr + avroStr + insertOnlyStr
  val avroBucketedInsertOnlyTable = clusteredStr + avroStr + insertOnlyStr

  private def generateTableVariations(fileFormatTypes: Array[String],
                                      partitionedTypes: Array[String],
                                      clusteredTypes: Array[String],
                                      acidTypes: Array[String]): List[(String, Boolean)] = {
    var tblTypes = new ListBuffer[(String, Boolean)]()
    for (fileFormat <- fileFormatTypes) {
      for (partitioned <- partitionedTypes) {
        for (clustered <- clusteredTypes) {
          for (acidType <- acidTypes) {
            val tType = partitioned + clustered + fileFormat + acidType
            if (partitioned != "") {
              tblTypes += ((tType, true))
            } else {
              tblTypes += ((tType, false))
            }
          }
        }
      }
    }
    tblTypes.filter {
      case (name, isPartitioned) =>
        // Filter out all non-orc, full acid tables
        !(!name.toLowerCase().contains("orc") && name.contains(fullAcidStr))
    }.toList
  }

  // Loop through all variations
  def allFullAcidTypes(): List[(String, Boolean)] = {
    val acidType = fullAcidStr
    val fileFormatTypes = Array(orcStr)
    val partitionedTypes = Array("", partitionedStr)
    val clusteredTypes = Array("", clusteredStr)

    generateTableVariations(fileFormatTypes, partitionedTypes, clusteredTypes, Array(acidType))
  }

  // Loop through all variations
  def allInsertOnlyTypes(): List[(String, Boolean)] = {
    val acidType = insertOnlyStr
    // NB: Avro not supported !!!
    val fileFormatTypes = Array(orcStr, parquetStr, textStr)
    val partitionedTypes = Array("", partitionedStr)
    val clusteredTypes = Array("", clusteredStr)

    generateTableVariations(fileFormatTypes, partitionedTypes, clusteredTypes, Array(acidType))
  }


  def hiveJoin(table1: Table, table2: Table): String = {
    s"SELECT * FROM ${table1.hiveTname} t1 JOIN ${table2.hiveTname} t2 WHERE t1.key = t2.key ORDER BY ${table1.hiveOrderBy("t1")} , ${table2.hiveOrderBy("t2")}"
  }

  def sparkJoin(table1: Table, table2: Table): String = {
    s"SELECT * FROM ${table1.sparkTname} t1 JOIN ${table2.sparkTname} t2 WHERE t1.key = t2.key ORDER BY ${table1.sparkOrderBy("t1")} , ${table2.sparkOrderBy("t2")}"
  }
}
