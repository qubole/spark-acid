package com.qubole.spark.hiveacid.streaming

import java.util.Locale

import com.qubole.spark.hiveacid.Table
import org.apache.spark.sql.streaming.OutputMode


class HiveAcidSinkOptionsSuite extends HiveAcidStreamingFunSuite {

  import HiveAcidSinkOptions._

  test("bad sink options") {

    def testBadOptions(options: List[(String, String)])(expectedMsg: String): Unit = {

      val tableName = "tempTable"
      val tType = Table.orcFullACIDTable
      val cols = Map(
        ("value1","int"),
        ("value2", "int")
      )
      val tableHive = new Table(DEFAULT_DBNAME, tableName, cols, tType, false)

        // creating table
        helper.recreate(tableHive)
        val errorMessage = intercept[IllegalArgumentException] {
          helper.runStreaming(
            tableHive.hiveTname, OutputMode.Append(), tableHive.getColMap.keys.toSeq, Range(1, 4), options)
        }.getMessage
        assert(errorMessage.toLowerCase(Locale.ROOT).contains(expectedMsg.toLowerCase(Locale.ROOT)))

    }

    testBadOptions(List(CLEANUP_DELAY_KEY -> "-2"))("Invalid value '-2' " +
      s"for option '$CLEANUP_DELAY_KEY', must be a positive integer")
    testBadOptions(List(COMPACT_INTERVAL_KEY -> "-5"))("Invalid value '-5' " +
      s"for option '$COMPACT_INTERVAL_KEY', must be a positive integer")
    testBadOptions(List(MIN_BATCHES_TO_RETAIN_KEY -> "-5"))("Invalid value '-5' " +
      s"for option '$MIN_BATCHES_TO_RETAIN_KEY', must be a positive integer")
    testBadOptions(List(LOG_DELETION_KEY -> "x"))("Invalid value 'x' " +
      s"for option '$LOG_DELETION_KEY', must be true or false")

  }

}
