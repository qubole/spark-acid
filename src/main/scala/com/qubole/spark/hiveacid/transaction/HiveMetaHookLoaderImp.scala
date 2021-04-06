package com.qubole.spark.hiveacid.transaction

import org.apache.hadoop.hive.metastore.HiveMetaHookLoader
import org.apache.hadoop.hive.metastore.HiveMetaHook
import org.apache.hadoop.hive.metastore.api.MetaException
import org.apache.hadoop.hive.metastore.api.Table

class HiveMetaHookLoaderImp extends HiveMetaHookLoader {

  @throws[MetaException]
  def getHook(tbl: Table): HiveMetaHook = null
}
