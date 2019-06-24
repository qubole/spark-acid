package com.qubole.spark.datasources.hiveacid.util

import org.apache.spark.unsafe.types.UTF8String

object InputFileBlockHolder {
  /**
    * A wrapper around some input file information.
    *
    * @param filePath path of the file read, or empty string if not available.
    * @param startOffset starting offset, in bytes, or -1 if not available.
    * @param length size of the block, in bytes, or -1 if not available.
    */
  private class FileBlock(val filePath: UTF8String, val startOffset: Long, val length: Long) {
    def this() {
      this(UTF8String.fromString(""), -1, -1)
    }
  }

  /**
    * The thread variable for the name of the current file being read. This is used by
    * the InputFileName function in Spark SQL.
    */
  private[this] val inputBlock: InheritableThreadLocal[FileBlock] =
    new InheritableThreadLocal[FileBlock] {
      override protected def initialValue(): FileBlock = new FileBlock
    }

  /**
    * Returns the holding file name or empty string if it is unknown.
    */
  def getInputFilePath: UTF8String = inputBlock.get().filePath

  /**
    * Returns the starting offset of the block currently being read, or -1 if it is unknown.
    */
  def getStartOffset: Long = inputBlock.get().startOffset

  /**
    * Returns the length of the block being read, or -1 if it is unknown.
    */
  def getLength: Long = inputBlock.get().length

  /**
    * Sets the thread-local input block.
    */
  def set(filePath: String, startOffset: Long, length: Long): Unit = {
    require(filePath != null, "filePath cannot be null")
    require(startOffset >= 0, s"startOffset ($startOffset) cannot be negative")
    require(length >= 0, s"length ($length) cannot be negative")
    inputBlock.set(new FileBlock(UTF8String.fromString(filePath), startOffset, length))
  }

  /**
    * Clears the input file block to default value.
    */
  def unset(): Unit = inputBlock.remove()
}
