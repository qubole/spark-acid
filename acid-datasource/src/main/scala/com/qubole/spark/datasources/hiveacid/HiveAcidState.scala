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

import com.qubole.shaded.hive.common.{ValidTxnWriteIdList, ValidWriteIdList}
import com.qubole.shaded.hive.conf.HiveConf
import com.qubole.shaded.hive.metastore.HiveMetaStoreClient
import com.qubole.shaded.hive.metastore.txn.TxnUtils
import com.qubole.shaded.hive.ql.metadata
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions._

class HiveAcidState(sparkSession: SparkSession,
                    val hiveConf: HiveConf,
                    val table: metadata.Table,
                    val sizeInBytes: Long,
                    val pSchema: StructType,
                    val heartbeatInterval: Long,
                    val isFullAcidTable: Boolean) extends Logging {

  val location: Path = table.getDataLocation
  private val dbName: String = table.getDbName
  private val tableName: String = table.getTableName
  private val txnId: Long = -1
  private var validWriteIdsNoTxn: ValidWriteIdList = _

  def beginRead: Unit = {
    // Get write ids to read. Currently, this data source does not open a transaction or take locks against
    // it's read entities(partitions). This can be enhanced in the future
    val client = new HiveMetaStoreClient(hiveConf, null, false)
    val validTxns = client.getValidTxns()
    val txnWriteIds: ValidTxnWriteIdList = TxnUtils.createValidTxnWriteIdList(txnId,
      client.getValidWriteIds(Seq(dbName + "." + tableName),
        validTxns.writeToString()))
    validWriteIdsNoTxn = txnWriteIds.getTableValidWriteIdList(table.getDbName + "." + table.getTableName)
    client.close()
  }

  def end(): Unit = {
    // no op for now. If we start taking locks in the future, this can be implemented to release the locks and
    // close the transaction
  }

  def getValidWriteIds: ValidWriteIdList = {
    if (validWriteIdsNoTxn == null) {
      throw HiveAcidErrors.validWriteIdsNotInitialized
    }
    validWriteIdsNoTxn
  }
}
