package com.qubole.shaded.hadoop.hive.ql.io.orc

import java.util.regex.Pattern

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.AcidUtils
import org.apache.hadoop.hive.ql.io.orc.{OrcSplit, OrcStruct}

object OrcAcidUtil {
  val BUCKET_PATTERN = Pattern.compile("bucket_[0-9]{5}$")

  /*def extractElement(orcStruct: OrcStruct, index : Int) : OrcStruct {
    null
  }*/

}
