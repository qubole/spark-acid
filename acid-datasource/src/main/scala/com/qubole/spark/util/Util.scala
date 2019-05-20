package com.qubole.spark.util

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

  def classForName(className: String): Class[_] = {
    Class.forName(className, true, Thread.currentThread().getContextClassLoader)
    // scalastyle:on classforname
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
