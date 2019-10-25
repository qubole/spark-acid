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
import com.qubole.shaded.hadoop.hive.ql.io.{HiveFileFormatUtils, RecordIdentifier, RecordUpdater, _}
import com.qubole.shaded.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import com.qubole.shaded.hadoop.hive.serde2.objectinspector.{ObjectInspectorFactory, ObjectInspectorUtils, StructObjectInspector}
import com.qubole.shaded.hadoop.hive.serde2.{Deserializer, SerDeUtils}
import com.qubole.spark.datasources.hiveacid.util.{Hive3Inspectors, Util}
import com.qubole.spark.datasources.hiveacid.{HiveAcidErrors, HiveAcidOperation}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{JobConf, Reporter}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions.{Cast, Concat, Expression, Literal, ScalaUDF, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.types.StringType

import scala.collection.JavaConverters._

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
 *
 * @param options - writer options to use
 * @param hive3Options - Hive3 related writer options.
 */
private[writer] class HiveAcidFullAcidWriter(val options: WriterOptions,
                                             val hive3Options: HiveAcidWriterOptions)
  extends Writer with Logging {

  private val partitionPathExpression: Expression = Concat(
    options.partitionColumns.zipWithIndex.flatMap { case (c, i) =>
      val partitionName = ScalaUDF(
        ExternalCatalogUtils.getPartitionPathString _,
        StringType,
        Seq(Literal(c.name), Cast(c, StringType, Option(options.timeZoneId))),
        Seq(true, true))
      if (i == 0) Seq(partitionName) else Seq(Literal(Path.SEPARATOR), partitionName)
    })

  private val getDataValues: InternalRow => UnsafeRow = {
    val proj = UnsafeProjection.create(options.dataColumns, options.allColumns)
    row => proj(row)
  }

  private val getPartitionValues: InternalRow => UnsafeRow = {
    val proj = UnsafeProjection.create(options.partitionColumns,
      options.allColumns)
    row => proj(row)
  }

  private val getPartitionPath: InternalRow => String = {
    val proj = UnsafeProjection.create(Seq(partitionPathExpression),
      options.partitionColumns)
    row => proj(row).getString(0)
  }

  private val jobConf = {
    val hConf = options.serializableHadoopConf.value
    new JobConf(hConf)
  }

  private val partitionsTouchedSet = scala.collection.mutable.HashSet[TablePartitionSpec]()

  def initializeNewRecordUpdater(path: Path, acidBucketId: Int): RecordUpdater = {
    val tableDesc = hive3Options.getFileSinkDesc.getTableInfo

    val rowIdColNum = options.operationType match {
      case HiveAcidOperation.INSERT_INTO | HiveAcidOperation.INSERT_OVERWRITE =>
        -1
      case x =>
        throw new RuntimeException(s"Invalid write operation $x")
    }

    val fileSinkConf = hive3Options.getFileSinkDesc
    fileSinkConf.setDirName(new Path(hive3Options.rootPath))

    val recordUpdater = HiveFileFormatUtils.getAcidRecordUpdater(
      jobConf,
      tableDesc,
      acidBucketId,
      hive3Options.getFileSinkDesc,
      path,
      sparkHiveRowConverter.getObjectInspector,
      Reporter.NULL,
      rowIdColNum)

    val acidOutputFormatOptions = new AcidOutputFormat.Options(jobConf)
      .writingBase(options.operationType == HiveAcidOperation.INSERT_OVERWRITE)
      .bucket(acidBucketId)
      .minimumWriteId(fileSinkConf.getTableWriteId)
      .maximumWriteId(fileSinkConf.getTableWriteId)
      .statementId(fileSinkConf.getStatementId)

    val (createDelta, createDeleteDelta) = options.operationType match {
      case HiveAcidOperation.INSERT_INTO | HiveAcidOperation.INSERT_OVERWRITE => (true, false)
      case unknownOperation => throw HiveAcidErrors.invalidOperationType(unknownOperation.toString)
    }

    val fs = path.getFileSystem(jobConf)

    def createVersionFile(acidOptions: AcidOutputFormat.Options): Unit = {
      try {
        AcidUtils.OrcAcidVersion.writeVersionFile(
          AcidUtils.createFilename(path, acidOptions).getParent, fs)
      } catch {
        case _: Exception =>
          logError("Version file already found")
        case scala.util.control.NonFatal(_) =>
          logError("Version file already found - non fatal")
        case _: Throwable =>
          logError("Version file already found - shouldn't be caught")
      }
    }

    if (createDelta) {
      createVersionFile(acidOutputFormatOptions)
    }

    if (createDeleteDelta) {
      createVersionFile(acidOutputFormatOptions.writingDeleteDelta(true))
    }
    recordUpdater
  }

  private val recordUpdaters = scala.collection.mutable.Map[(String, Int), RecordUpdater]()
  private def getRecordUpdaterForPartition(partitionRow: InternalRow): RecordUpdater = {
    val path = if (options.partitionColumns.isEmpty) {
      new Path(hive3Options.rootPath)
    } else {
      val partitionPath = getPartitionPath(partitionRow)
      partitionsTouchedSet.add(PartitioningUtils.parsePathFragment(partitionPath))
      new Path(hive3Options.rootPath, partitionPath)
    }
    val acidBucketId = Utilities.getTaskIdFromFilename(TaskContext.get.taskAttemptId().toString)
      .toInt

    recordUpdaters.getOrElseUpdate((path.toUri.toString, acidBucketId),
      initializeNewRecordUpdater(path, acidBucketId))
  }

  private val sparkHiveRowConverter = new SparkHiveRowConverter(options, hive3Options, jobConf)

  private val hiveRow = new Array[Any](sparkHiveRowConverter.numFields)

  /**
   * Process an Spark InternalRow
   *
   * @param row row to be processed
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

    options.operationType match {
      case HiveAcidOperation.INSERT_INTO | HiveAcidOperation.INSERT_OVERWRITE =>
        recordUpdater.insert(options.currentWriteId, hiveRow)
      case x =>
        throw new RuntimeException(s"Invalid write operation $x")
    }

  }

  def close(): Unit = {
    // Seems the boolean value passed into close does not matter.
    // hiveWriter.close(false)
    recordUpdaters.foreach(_._2.close(false))
  }

  override def partitionsTouched(): Seq[TablePartitionSpec] = partitionsTouchedSet.toSeq
}

/**
  * This class is responsible for writing a InternalRow into a insert-only table
  * This can handle InsertInto/InsertOverwrite. This does not support Update/Delete operations
  * It has method `process` which takes 1 InternalRow and processes it based on
  * OperationType. row is expected to contain data and no row ids
  *
  * @param options writer options to use
  * @param hive3Options hive3 specific options, which is passed into underlying hive3 API
  */
private class HiveAcidInsertOnlyWriter(options: WriterOptions,
                                       hive3Options: HiveAcidWriterOptions)
  extends Writer {
  override def process(row: InternalRow): Unit = {}

  override def close(): Unit = {}

  override def partitionsTouched(): Seq[TablePartitionSpec] = Seq()
}

/**
 * Utility class to convert a spark InternalRow to a row required by Hive
 * @param options - hive acid writer options
 * @param jobConf - job conf
 */
private class SparkHiveRowConverter(options: WriterOptions,
                                    hive3Options: HiveAcidWriterOptions,
                                    jobConf: JobConf) extends Hive3Inspectors {

  private val tableDesc = hive3Options.getFileSinkDesc.getTableInfo
  private lazy val deserializerClass = Util.classForName(tableDesc.getSerdeClassName,
    loadShaded = true).asInstanceOf[Class[Deserializer]]

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

  private val objectInspector = options.operationType match {
    case HiveAcidOperation.INSERT_INTO | HiveAcidOperation.INSERT_OVERWRITE =>
      objectInspectorWithoutRowId
    case x =>
      throw new RuntimeException(s"Invalid write operation $x")
  }

  private val fieldOIs =
    objectInspector.getAllStructFieldRefs.asScala.map(_.getFieldObjectInspector).toArray
  private val dataTypes = options.dataColumns.map(_.dataType).toArray
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
