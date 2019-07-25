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

package com.qubole.spark.datasources.hiveacid.util

import java.io.IOException

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileSplit, InputSplit}
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit

import org.apache.spark.internal.Logging

import scala.util.control.NonFatal


object SplitFileSystemType extends Enumeration {
  type SplitFileSystemType = Value
  val NONE, BLOB_STORE= Value
}

object Util extends Logging {

  def classForName(className: String, loadShaded: Boolean = false): Class[_] = {
    val classToLoad = if (loadShaded) {
      className.replaceFirst("org.apache.hadoop.hive.", "com.qubole.shaded.hadoop.hive.")
    } else {
      className
    }
    Class.forName(classToLoad, true, Thread.currentThread().getContextClassLoader)
  }

  val fileSystemSchemes: List[String] = List("s3", "s3n", "s3a", "wasb", "adl",
    "oraclebmc", "oci")

  def inShutdown(): Boolean = {
    try {
      val hook = new Thread {
        override def run() {}
      }
      // scalastyle:off runtimeaddshutdownhook
      Runtime.getRuntime.addShutdownHook(hook)
      // scalastyle:on runtimeaddshutdownhook
      Runtime.getRuntime.removeShutdownHook(hook)
    } catch {
      case ise: IllegalStateException => return true
    }
    false
  }

  def getSplitScheme(path: String) : SplitFileSystemType.SplitFileSystemType = {
    if(path == null) {
      return SplitFileSystemType.NONE
    }
    if (fileSystemSchemes.contains(new Path(path).toUri.getScheme.toLowerCase)) {
      return SplitFileSystemType.BLOB_STORE
    }
    return SplitFileSystemType.NONE
  }

  def getSplitScheme[T >: InputSplit](split: T) : SplitFileSystemType.SplitFileSystemType = {
    split match {
      case f: FileSplit =>
        if (fileSystemSchemes.contains(
          split.asInstanceOf[FileSplit].getPath.toUri.getScheme.toLowerCase)) {
          SplitFileSystemType.BLOB_STORE
        } else {
          SplitFileSystemType.NONE
        }
      // When wholeTextFiles is used for reading multiple files in one go,
      // the split has multiple paths in it. We get the scheme of the first
      // path and use that for the rest too.
      case cf : CombineFileSplit =>
        if (fileSystemSchemes.contains(
          split.asInstanceOf[CombineFileSplit].getPath(0).toUri.getScheme.toLowerCase)) {
          SplitFileSystemType.BLOB_STORE
        } else {
          SplitFileSystemType.NONE
        }
      case _ => SplitFileSystemType.NONE
    }
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
