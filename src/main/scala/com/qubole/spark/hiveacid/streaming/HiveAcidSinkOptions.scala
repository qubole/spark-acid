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

package com.qubole.spark.hiveacid.streaming

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import scala.util.Try

class HiveAcidSinkOptions(parameters: CaseInsensitiveMap[String]) {

  import HiveAcidSinkOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  val tableName = parameters.get("table").getOrElse{
    throw new IllegalArgumentException("Table Name is not specified")
  }

  val fileCleanupDelayMs = withLongParameter(CLEANUP_DELAY_KEY, DEFAULT_CLEANUP_DELAY)

  val isDeletingExpiredLog = withBooleanParameter(LOG_DELETION_KEY, DEFAULT_LOG_DELETION)

  val compactInterval = withIntParameter(COMPACT_INTERVAL_KEY, DEFAULT_COMPACT_INTERVAL)

  val minBatchesToRetain = withIntParameter(MIN_BATCHES_TO_RETAIN_KEY, DEFAULT_MIN_BATCHES_TO_RETAIN)

  val metadataDir = parameters.get(METADATA_DIR_KEY)

  private def withIntParameter(name: String, default: Int): Int = {
    parameters.get(name).map { str =>
      Try(str.toInt).toOption.filter(_ > 0).getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value '$str' for option '$name', must be a positive integer")
      }
    }.getOrElse(default)
  }

  private def withLongParameter(name: String, default: Long): Long = {
    parameters.get(name).map { str =>
      Try(str.toLong).toOption.filter(_ >= 0).getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value '$str' for option '$name', must be a positive integer")
      }
    }.getOrElse(default)
  }

  private def withBooleanParameter(name: String, default: Boolean): Boolean = {
    parameters.get(name).map { str =>
      try {
        str.toBoolean
      } catch {
        case _: IllegalArgumentException =>
          throw new IllegalArgumentException(
            s"Invalid value '$str' for option '$name', must be true or false")
      }
    }.getOrElse(default)
  }

}

object HiveAcidSinkOptions {

  val DEFAULT_CLEANUP_DELAY = TimeUnit.MINUTES.toMillis(10)
  val DEFAULT_LOG_DELETION = true
  val DEFAULT_COMPACT_INTERVAL = 10
  val DEFAULT_MIN_BATCHES_TO_RETAIN = 100

  val CLEANUP_DELAY_KEY = "spark.acid.streaming.log.cleanupDelayMs"
  val LOG_DELETION_KEY = "spark.acid.streaming.log.deletion"
  val COMPACT_INTERVAL_KEY = "spark.acid.streaming.log.compactInterval"
  val MIN_BATCHES_TO_RETAIN_KEY = "spark.acid.streaming.log.minBatchesToRetain"
  val METADATA_DIR_KEY = "spark.acid.streaming.log.metadataDir"

}