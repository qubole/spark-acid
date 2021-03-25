package com.qubole.shaded.hadoop.hive.ql.io.orc

import java.util.regex.Pattern

import com.qubole.shaded.hadoop.hive.ql.io.AcidUtils
import org.apache.hadoop.fs.Path

object OrcAcidUtil {
  val BUCKET_PATTERN = Pattern.compile("bucket_[0-9]{5}$")

  //we dont need this method since hive 2.1 doesn't create delete_delta dirs for delete/update operations
  /*def getDeleteDeltaPaths(orcSplit: OrcSplit): Array[Path] = {
    assert(BUCKET_PATTERN.matcher(orcSplit.getPath.getName).matches())
    val bucket = AcidUtils.parseBucketId(orcSplit.getPath)
    assert(bucket != -1)
    val deleteDeltaDirPaths = VectorizedOrcAcidRowBatchReader.getDeleteDeltaDirsFromSplit(orcSplit);
    deleteDeltaDirPaths.map(deleteDir => AcidUtils.createBucketFile(deleteDir, bucket))

  }*/
}
