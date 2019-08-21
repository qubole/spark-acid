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

package com.qubole.spark.datasources.hiveacid.writer

import java.util.Properties

import com.qubole.shaded.hadoop.hive.ql.exec.Utilities
import com.qubole.shaded.hadoop.hive.ql.io.{HiveFileFormatUtils, RecordIdentifier, RecordUpdater}
import com.qubole.shaded.hadoop.hive.ql.plan.FileSinkDesc
import com.qubole.shaded.hadoop.hive.serde2.{Deserializer, SerDeUtils}
import com.qubole.shaded.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import com.qubole.shaded.hadoop.hive.serde2.objectinspector.{ObjectInspectorFactory, ObjectInspectorUtils, StructObjectInspector}
import com.qubole.spark.datasources.hiveacid.{HiveAcidMetadata, HiveAcidOperation}
import com.qubole.spark.datasources.hiveacid.util.{Hive3Inspectors, SerializableConfiguration, Util}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{JobConf, Reporter}
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Concat, Expression, Literal, ScalaUDF, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.StringType

import scala.collection.JavaConverters._
import scala.collection.mutable

private[writer] trait RowWriter {
  def writerOptions: RowWriterOptions
  def process(row: InternalRow): Unit
  def close(): Unit
}

private[writer] object RowWriter {
  def getRowWriter(writerOptions: RowWriterOptions,
                   isFullAcidTable: Boolean): RowWriter = {
    if (isFullAcidTable) {
      new FullAcidRowWriter(writerOptions)
    } else {
      new InsertOnlyRowWriter(writerOptions)
    }
  }
}

/**
 * This class is responsible for writing a InternalRow into a Full-ACID table
 * This can handle InsertInto/InsertOverwrite/Update/Delete operations
 * It has method `process` which takes 1 InternalRow and processes it based on
 * OperationType (insert/update/delete etc).
 *
 * It is assumed that the InternalRow passed to process is in the right form
 * i.e.
 * for InsertInto/InsertOverwrite operations, row is expected to contain data and no row ids
 * for Delete, row is expected to contain rowId
 * for Update, row is expected to contain data as well as row Id
 * @param writerOptions - writer options to use
 */
private class FullAcidRowWriter(val writerOptions: RowWriterOptions)
  extends RowWriter {

  private val partitionPathExpression: Expression = Concat(
    writerOptions.partitionColumns.zipWithIndex.flatMap { case (c, i) =>
      val partitionName = ScalaUDF(
        ExternalCatalogUtils.getPartitionPathString _,
        StringType,
        Seq(Literal(c.name), Cast(c, StringType, Option(writerOptions.timeZoneId))),
        Seq(true, true))
      if (i == 0) Seq(partitionName) else Seq(Literal(Path.SEPARATOR), partitionName)
    })

  private val getDataValues: InternalRow => UnsafeRow = {
    val proj = UnsafeProjection.create(writerOptions.dataColumns, writerOptions.allColumns)
    row => proj(row)
  }

  private val getPartitionValues: InternalRow => UnsafeRow = {
    val proj = UnsafeProjection.create(writerOptions.partitionColumns,
      writerOptions.allColumns)
    row => proj(row)
  }

  private val getPartitionPath: InternalRow => String = {
    val proj = UnsafeProjection.create(Seq(partitionPathExpression),
      writerOptions.partitionColumns)
    row => proj(row).getString(0)
  }

  private val jobConf = {
    val hConf = writerOptions.serializableHadoopConf.value
    new JobConf(hConf)
  }

  def initializeNewRecordUpdater(path: Path, acidBucketId: Int): RecordUpdater = {
    val tableDesc = writerOptions.fileSinkConf.getTableInfo

    val rowIdColNum = writerOptions.operationType match {
      case HiveAcidOperation.DELETE | HiveAcidOperation.UPDATE =>
        0
      case HiveAcidOperation.INSERT_INTO | HiveAcidOperation.INSERT_OVERWRITE =>
        -1
      case x =>
        throw new RuntimeException(s"Invalid write operation $x")
    }

    HiveFileFormatUtils.getAcidRecordUpdater(
      jobConf,
      tableDesc,
      acidBucketId,
      writerOptions.fileSinkConf,
      path,
      sparkHiveRowConverter.getObjectInspector,
      Reporter.NULL,
      rowIdColNum)
  }

  private val recordUpdaters = scala.collection.mutable.Map[(String, Int), RecordUpdater]()
  private def getRecordUpdaterForPartition(partitionRow: InternalRow): RecordUpdater = {
    val path = if (writerOptions.partitionColumns.isEmpty) {
      new Path(writerOptions.rootPath)
    } else {
      new Path(writerOptions.rootPath, getPartitionPath(partitionRow))
    }
    val acidBucketId = Utilities.getTaskIdFromFilename(TaskContext.get.taskAttemptId().toString)
      .toInt

    recordUpdaters.getOrElseUpdate((path.toUri.toString, acidBucketId),
      initializeNewRecordUpdater(path, acidBucketId))
  }

  private val sparkHiveRowConverter = new SparkHiveRowConverter(writerOptions, jobConf)

  //  private val hiveWriter = HiveFileFormatUtils.getHiveRecordWriter(
  //    jobConf,
  //    tableDesc,
  //    serializer.getSerializedClass,
  //    fileSinkConf,
  //    new Path(path),
  //    Reporter.NULL)
  //  private val serializer = {
  //    val serializer = deserializerClass.newInstance().asInstanceOf[Serializer]
  //    serializer.initialize(jobConf, tableDesc.getProperties)
  //    serializer
  //  }

  private val hiveRow = new Array[Any](sparkHiveRowConverter.numFields)

  /**
    * Process an Spark InternalRow
    *
    * @param row
    */
  def process(row: InternalRow): Unit = {
    //  Identify the partitionColumns and nonPartitionColumns in row
    val partitionColRow = getPartitionValues(row)
    val dataColRow = getDataValues(row)

    //  Get the recordUpdater for  this partitionedRow
    val recordUpdater = getRecordUpdaterForPartition(partitionColRow)

    sparkHiveRowConverter.buildHiveRow(dataColRow, hiveRow)
    // val serializedRow = serializer.serialize(outputData, standardOI)
    // hiveWriter.write(serializedRow)

    writerOptions.operationType match {
      case HiveAcidOperation.DELETE =>
        recordUpdater.delete(writerOptions.currentWriteId, hiveRow)
      case HiveAcidOperation.UPDATE =>
        recordUpdater.update(writerOptions.currentWriteId, hiveRow)
      case HiveAcidOperation.INSERT_INTO | HiveAcidOperation.INSERT_OVERWRITE =>
        recordUpdater.insert(writerOptions.currentWriteId, hiveRow)
      case x =>
        throw new RuntimeException(s"Invalid write operation $x")
    }

  }

  def close(): Unit = {
    // Seems the boolean value passed into close does not matter.
    // hiveWriter.close(false)
    recordUpdaters.foreach(_._2.close(false))
  }
}

private class InsertOnlyRowWriter(val writerOptions: RowWriterOptions)
  extends RowWriter {
  override def process(row: InternalRow): Unit = {}

  override def close(): Unit = {}
}

/**
 * Utility class to conver a spark InternalRow to a row required by Hive
 * @param writerOptions - hive acid writer options
 * @param jobConf - job conf
 */
private class SparkHiveRowConverter(writerOptions: RowWriterOptions,
                                    jobConf: JobConf) extends Hive3Inspectors {

  private val tableDesc = writerOptions.fileSinkConf.getTableInfo
  private lazy val deserializerClass = Util.classForName(tableDesc.getSerdeClassName,
    true).asInstanceOf[Class[Deserializer]]

  private val objectInspectorWithoutRowId = {
    // Can't use tableDesc.getDeserializer as it  uses Reflection
    // internally which doesn't work because of shading. So copied its logic
    val deserializer = deserializerClass.newInstance()
    SerDeUtils.initializeSerDe(
      deserializer, jobConf, tableDesc.getProperties, null.asInstanceOf[Properties])
    ObjectInspectorUtils
      .getStandardObjectInspector(
        deserializer.getObjectInspector,
        ObjectInspectorCopyOption.JAVA)
      .asInstanceOf[StructObjectInspector]
  }

  private val recordIdentifierInspector = RecordIdentifier.StructInfo.oi

  private val objectInspectorWithRowId = {
    val dataStructFields = asScalaIteratorConverter(
      objectInspectorWithoutRowId.getAllStructFieldRefs.iterator).asScala.toSeq

    val newFieldNameSeq = Seq("rowId") ++ dataStructFields.map(_.getFieldName)
    val newOISeq = Seq(recordIdentifierInspector) ++
      dataStructFields.map(_.getFieldObjectInspector)
    ObjectInspectorFactory.getStandardStructObjectInspector(
      newFieldNameSeq.asJava, newOISeq.asJava
    )
  }

  private val objectInspector = writerOptions.operationType match {
    case HiveAcidOperation.DELETE =>
      objectInspectorWithRowId
    case HiveAcidOperation.UPDATE =>
      objectInspectorWithRowId
    case HiveAcidOperation.INSERT_INTO | HiveAcidOperation.INSERT_OVERWRITE =>
      objectInspectorWithoutRowId
    case x =>
      throw new RuntimeException(s"Invalid write operation $x")
  }

  private val fieldOIs =
    objectInspector.getAllStructFieldRefs.asScala.map(_.getFieldObjectInspector).toArray
  private val dataTypes = writerOptions.dataColumns.map(_.dataType).toArray
  private val wrappers = fieldOIs.zip(dataTypes).map { case (f, dt) => wrapperFor(f, dt) }

  def getObjectInspector: StructObjectInspector = objectInspector

  def numFields: Int = fieldOIs.length

  def buildHiveRow(sparkRow: InternalRow, hiveRow: Array[Any]): Unit = {
    var i = 0
    while (i < fieldOIs.length) {
      hiveRow(i) = if (sparkRow.isNullAt(i)) null else wrappers(i)(sparkRow.get(i, dataTypes(i)))
      i += 1
    }
  }
}

