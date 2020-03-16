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

import org.apache.log4j.{Level, LogManager, Logger}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.control.NonFatal


abstract class HiveAcidStreamingFunSuite extends FunSuite with BeforeAndAfterAll {

  protected val log: Logger = LogManager.getLogger(this.getClass)
  log.setLevel(Level.INFO)

  protected var helper: StreamingTestHelper = _
  protected val isDebug = true

  protected val DEFAULT_DBNAME =  "HiveTestDB"

  override protected def beforeAll() {
    try {

      helper = new StreamingTestHelper
      if (isDebug) {
        log.setLevel(Level.DEBUG)
      }
      helper.init(isDebug)

      // DB
      helper.hiveExecute("DROP DATABASE IF EXISTS "+ DEFAULT_DBNAME +" CASCADE")
      helper.hiveExecute("CREATE DATABASE "+ DEFAULT_DBNAME)
    } catch {
      case NonFatal(e) => log.info("failed " + e)
    }
  }

  override protected def afterAll(): Unit = {
    helper.destroy()
  }

}
