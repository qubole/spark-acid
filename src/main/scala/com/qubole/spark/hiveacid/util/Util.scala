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

package com.qubole.spark.hiveacid.util

import java.io.IOException

import org.apache.spark.internal.Logging

import scala.util.control.NonFatal

private[hiveacid] object Util extends Logging {

  def classForName(className: String, loadShaded: Boolean = false): Class[_] = {
    val classToLoad = if (loadShaded) {
      className.replaceFirst("org.apache.hadoop.hive.", "com.qubole.shaded.hadoop.hive.")
    } else {
      className
    }
    Class.forName(classToLoad, true, Thread.currentThread().getContextClassLoader)
  }

  /**
    * Detect whether this thread might be executing a shutdown hook. Will always return true if
    * the current thread is a running a shutdown hook but may spuriously return true otherwise (e.g.
    * if System.exit was just called by a concurrent thread).
    *
    * Currently, this detects whether the JVM is shutting down by Runtime#addShutdownHook throwing
    * an IllegalStateException.
    */
  def inShutdown(): Boolean = {
    try {
      val hook: Thread = new Thread {
        override def run() {}
      }
      // scalastyle:off runtimeaddshutdownhook
      Runtime.getRuntime.addShutdownHook(hook)
      // scalastyle:on runtimeaddshutdownhook
      Runtime.getRuntime.removeShutdownHook(hook)
    } catch {
      case _: IllegalStateException => return true
    }
    false
  }

  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }
}
