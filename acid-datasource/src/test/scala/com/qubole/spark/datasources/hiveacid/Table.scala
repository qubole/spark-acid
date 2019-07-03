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
  private val isPartitioned: Boolean = false) {

    private var colMap = Map("key" -> "int") ++ extraColMap

    // NB Add date column as well apparently always in the end
    private def getRow(key: Int): String = colMap.map( x => {
      x._2 match {
        case "date" => s"'${(new DateTime(((key * 1000L) + 151502791900L))).toString}'"
        case _ => key.toString
      }}).mkString(", ") + {if (isPartitioned) s", '${(new DateTime(((key * 1000L) + 151502791900L))).toString}'" else ""}

    private def getColDefString = colMap.map(x => x._1 + " " + x._2).mkString(",")

    // FIXME: Add load_date column of partitioned table in order by clause
    private def sparkOrderBy = colMap.map(x => s"${sparkTname}.${x._1}").mkString(", ") + {if (isPartitioned) s", ${sparkTname}.load_date " else ""}
    private def hiveOrderBy = colMap.map(x => s"$tName.${x._1}").mkString(", ") + {if (isPartitioned) s", ${tName}.load_date" else ""}
    private def getCols = colMap.map(x => x._1).mkString(", ") + {if (isPartitioned) ", load_date" else ""}

    def getColMap = colMap

    def hiveTname = s"$dbName.$tName"
    def hiveTname1 = s"$tName"
    def sparkTname = s"${dbName}.spark_${tName}"

    def hiveCreate = s"CREATE TABLE ${hiveTname} (${getColDefString}) ${tblProp}"
    def hiveSelect = s"SELECT * FROM ${hiveTname} ORDER BY ${hiveOrderBy}"
    def hiveSelectWithPred = s"SELECT * FROM ${hiveTname} with intCol < 5 ORDER BY ${hiveOrderBy}"
    def hiveSelectWithProj = s"SELECT intCol FROM ${hiveTname} ORDER BY intCol"
    def hiveDrop = s"DROP TABLE IF EXISTS ${hiveTname}"

    def sparkCreate = s"CREATE TABLE ${sparkTname} USING HiveAcid OPTIONS('table' '${hiveTname}')"
    def sparkSelect = s"SELECT * FROM ${sparkTname} ORDER BY ${sparkOrderBy}"
    def sparkSelectWithPred = s"SELECT * FROM ${sparkTname} where intCol < 5 ORDER BY ${sparkOrderBy}"
    def sparkSelectWithProj = s"SELECT intCol FROM ${sparkTname} ORDER BY intCol"
    def sparkDFProj = "intCol"
    def sparkDFPred = "\"intCol\" < \"5\""
    def sparkDrop = s"DROP TABLE IF EXISTS ${sparkTname}"

    def insertIntoHiveTableKeyRange(startKey: Int, endKey: Int): String =
      s"INSERT INTO TABLE ${hiveTname} (${getCols}) " + (startKey to endKey).map { key => s" select ${getRow(key)} " }.mkString(" UNION ALL ")
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
    def minorCompaction = s"ALTER TABLE ${hiveTname} COMPACT 'minor'"
    def majorCompaction = s"ALTER TABLE ${hiveTname} COMPACT 'major'"

}

object Table {

  // Create table string builder
  // 1st param
  private val partitionedStr = "PARTITIONED BY (load_date int) "
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

  val orcFullACIDTable = orcStr + fullAcidStr
  val orcPartitionedFullACIDTable = partitionedStr + orcStr + fullAcidStr
  val orcBucketedPartitionedFullACIDTable = clusteredStr + partitionedStr + orcStr + fullAcidStr
  val orcBucketedFullACIDTable = clusteredStr + orcStr + fullAcidStr

  val parquetFullACIDTable = parquetStr + fullAcidStr
  val parquetPartitionedFullACIDTable = partitionedStr + parquetStr + fullAcidStr
  val parquetBucketedPartitionedFullACIDTable = clusteredStr + partitionedStr + parquetStr + fullAcidStr
  val parquetBucketedFullACIDTable = clusteredStr + parquetStr + fullAcidStr

  val textFullACIDTable = textStr + fullAcidStr
  val textPartitionedFullACIDTable = partitionedStr + textStr + fullAcidStr
  val textBucketedPartitionedFullACIDTable = clusteredStr + partitionedStr + textStr + fullAcidStr
  val textBucketedFullACIDTable = clusteredStr + textStr + fullAcidStr

  val avroFullACIDTable = avroStr + fullAcidStr
  val avroPartitionedFullACIDTable = partitionedStr + avroStr + fullAcidStr
  val avroBucketedPartitionedFullACIDTable = clusteredStr + partitionedStr + avroStr + fullAcidStr
  val avroBucketedFullACIDTable = clusteredStr + avroStr + fullAcidStr

  val orcInsertOnlyTable = orcStr + insertOnlyStr
  val orcPartitionedInsertOnlyTable = partitionedStr + orcStr + insertOnlyStr
  val orcBucketedPartitionedInsertOnlyTable = clusteredStr + partitionedStr + orcStr + insertOnlyStr
  val orcBucketedInsertOnlyTable = clusteredStr + orcStr + insertOnlyStr

  val parquetInsertOnlyTable = parquetStr + insertOnlyStr
  val parquetPartitionedInsertOnlyTable = partitionedStr + parquetStr + insertOnlyStr
  val parquetBucketedPartitionedInsertOnlyTable = clusteredStr + partitionedStr + parquetStr + insertOnlyStr
  val parquetBucketedInsertOnlyTable = clusteredStr + parquetStr + insertOnlyStr

  val textInsertOnlyTable = textStr + insertOnlyStr
  val textPartitionedInsertOnlyTable = partitionedStr + textStr + insertOnlyStr
  val textBucketedPartitionedInsertOnlyTable = clusteredStr + partitionedStr + textStr + insertOnlyStr
  val textBucketedInsertOnlyTable = clusteredStr + textStr + insertOnlyStr

  val avroInsertOnlyTable = avroStr + insertOnlyStr
  val avroPartitionedInsertOnlyTable = partitionedStr + avroStr + insertOnlyStr
  val avroBucketedPartitionedInsertOnlyTable = clusteredStr + partitionedStr + avroStr + insertOnlyStr
  val avroBucketedInsertOnlyTable = clusteredStr + avroStr + insertOnlyStr

  // Loop through all variations
  def allFullAcidTypes(): List[(String, Boolean)] = {
    val acidType = fullAcidStr
    val fileFormatTypes = Array(orcStr)
    //val partitionedTypes = Array("", partitionedStr)
    val partitionedTypes = Array("")
    val clusteredTypes = Array("", clusteredStr)

    var tblTypes = new ListBuffer[(String, Boolean)]()
    for (fileFormat <- fileFormatTypes) {
      for (partitioned <- partitionedTypes) {
        for (clustered <- clusteredTypes) {

          if ((fileFormat == parquetStr) || (fileFormat == textStr) || (fileFormat == avroStr)) {
            // UnSupported Combo
          } else {
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
    return tblTypes.toList
  }

  // Loop through all variations
  def allInsertOnlyTypes(): List[(String, Boolean)] = {
    val acidType = insertOnlyStr
    // NB: Avro not supported !!!
    val fileFormatTypes = Array(orcStr, parquetStr, textStr)
   // val partitionedTypes = Array("", partitionedStr)
    val partitionedTypes = Array("")
    val clusteredTypes = Array("", clusteredStr)

    var tblTypes = new ListBuffer[(String, Boolean)]()
    for (fileFormat <- fileFormatTypes) {
      for (partitioned <- partitionedTypes) {
        for (clustered <- clusteredTypes) {
          val tType = partitioned + clustered + fileFormat + acidType
          if (partitioned != "") {
            tblTypes += ((tType, true))
          } else {
            tblTypes += ((tType, false))
          }
        }
      }
    }
    return tblTypes.toList
  }

  def hiveJoin(table1: Table, table2: Table): String = {
    s"SELECT * FROM ${table1.hiveTname} JOIN ${table2.hiveTname} WHERE ${table1.hiveTname1}.key = ${table2.hiveTname1}.key ORDER BY ${table1.hiveOrderBy} , ${table2.hiveOrderBy}"
  }

  def sparkJoin(table1: Table, table2: Table): String = {
    s"SELECT * FROM ${table1.sparkTname} JOIN ${table2.sparkTname} WHERE ${table1.sparkTname}.key = ${table2.sparkTname}.key ORDER BY ${table1.sparkOrderBy} , ${table2.sparkOrderBy}"
  }
}
