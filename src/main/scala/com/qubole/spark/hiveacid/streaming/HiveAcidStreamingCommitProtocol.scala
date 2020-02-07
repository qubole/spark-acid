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

package com.qubole.spark.hiveacid.streaming

import org.apache.spark.internal.Logging

class HiveAcidStreamingCommitProtocol(fileLog: HiveAcidSinkLog) extends Serializable with Logging {

  import HiveAcidStreamingCommitProtocol._

  def commitJob(batchId: Long, txnId: Long): Unit = {

    def commitJobRetry(retryRemaining: Int, f: () => Unit): Boolean = {
      var retry = false
      try {
        f()
      }
      catch {
        case ie: IllegalStateException if ie.getMessage.contains("Race while writing batch") =>
          throw ie
        case e: Exception =>
          if (retryRemaining > 0) {
            logError(s"Unexpected error while writing commit file for batch $batchId ... " +
              s"Retrying", e)
            retry = true
          } else {
            logError(s"Unexpected error while writing commit file for batch $batchId ... " +
              s"Max retries reached", e)
            throw e
          }
      }
      retry
    }

    val array = Array(HiveAcidSinkStatus(txnId, HiveAcidSinkLog.ADD_ACTION))

    val commitJobAttempt = () => {
      if (fileLog.add(batchId, array)) {
        logInfo(s"Committed batch $batchId")
      } else {
        throw new IllegalStateException(s"Race while writing batch $batchId")
      }
    }

    var sleepSec = 1
    var retryRemaining = MAX_COMMIT_JOB_RETRIES - 1
    while (commitJobRetry(retryRemaining, commitJobAttempt)) {
      retryRemaining = retryRemaining - 1
      Thread.sleep(sleepSec * 1000)
      sleepSec = sleepSec * EXPONENTIAL_BACK_OFF_FACTOR
    }

  }

}

object HiveAcidStreamingCommitProtocol {

  val MAX_COMMIT_JOB_RETRIES = 3
  val EXPONENTIAL_BACK_OFF_FACTOR = 2

}