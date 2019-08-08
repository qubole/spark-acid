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
import com.qubole.shaded.hadoop.hive.ql.io.{HiveFileFormatUtils, RecordUpdater}
import com.qubole.shaded.hadoop.hive.ql.plan.FileSinkDesc
import com.qubole.shaded.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import com.qubole.shaded.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils, StructObjectInspector}
import com.qubole.shaded.hadoop.hive.serde2.{Deserializer, SerDeUtils}
import com.qubole.spark.datasources.hiveacid.HiveAcidOperation
import com.qubole.spark.datasources.hiveacid.rdd.Hive3Inspectors
import com.qubole.spark.datasources.hiveacid.util.{SerializableConfiguration, Util}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{JobConf, Reporter}
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Concat, Expression, Literal, ScalaUDF, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.StringType

import scala.collection.JavaConverters._
import scala.language.implicitConversions


class HiveAcidWriterOptions(val currentWriteId: Long,
                            val operationType: HiveAcidOperation.OperationType,
                            val fileSinkConf: FileSinkDesc,
                            val serializableHadoopConf: SerializableConfiguration,
                            val dataColumns: Seq[Attribute],
                            val partitionColumns: Seq[Attribute],
                            val allColumns: Seq[Attribute],
                            val rootPath: String,
                            val timeZoneId: String) extends Serializable

class HiveAcidWriter(writerOptions: HiveAcidWriterOptions)
  extends Hive3Inspectors {

  private val partitionPathExpression: Expression = Concat(
    writerOptions.partitionColumns.zipWithIndex.flatMap { case (c, i) =>
      val partitionName = ScalaUDF(
        ExternalCatalogUtils.getPartitionPathString _,
        StringType,
        Seq(Literal(c.name), Cast(c, StringType, Option(writerOptions.timeZoneId))),
        Seq(true, true))
      if (i == 0) Seq(partitionName) else Seq(Literal(Path.SEPARATOR), partitionName)
    })

  val getDataValues: InternalRow => UnsafeRow = {
    val proj = UnsafeProjection.create(writerOptions.dataColumns, writerOptions.allColumns)
    row => proj(row)
  }

  val getPartitionValues: InternalRow => UnsafeRow = {
    val proj = UnsafeProjection.create(writerOptions.partitionColumns,
      writerOptions.allColumns)
    row => proj(row)
  }

  val getPartitionPath: InternalRow => String = {
    val proj = UnsafeProjection.create(Seq(partitionPathExpression),
      writerOptions.partitionColumns)
    row => proj(row).getString(0)
  }


  private val tableDesc = writerOptions.fileSinkConf.getTableInfo
  private lazy val deserializerClass = Util.classForName(tableDesc.getSerdeClassName,
    true).asInstanceOf[Class[Deserializer]]
  private val jobConf = {
    val hConf = writerOptions.serializableHadoopConf.value
    new JobConf(hConf)
  }
  private val rootPath = writerOptions.rootPath

  private val recordUpdaters = scala.collection.mutable.Map[String, RecordUpdater]()
  private def getRecordUpdaterForPartition(partitionRow: InternalRow): RecordUpdater = {
    val path = if (writerOptions.partitionColumns.isEmpty) {
      new Path(rootPath)
    } else {
      new Path(rootPath, getPartitionPath(partitionRow))
    }

    def initializeNewRecordUpdater(): RecordUpdater = {
      val acidBucketId = Utilities.getTaskIdFromFilename(TaskContext.get.taskAttemptId().toString)
        .toInt
      val inspector: StructObjectInspector = standardOI
      val rowIdColNum = -1

      HiveFileFormatUtils.getAcidRecordUpdater(
        jobConf,
        tableDesc,
        acidBucketId,
        writerOptions.fileSinkConf,
        path,
        inspector,
        Reporter.NULL,
        rowIdColNum)
    }

    recordUpdaters.getOrElseUpdate(path.toUri.toString, initializeNewRecordUpdater)
  }

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

  private val standardOI = {
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

  private val fieldOIs =
    standardOI.getAllStructFieldRefs.asScala.map(_.getFieldObjectInspector).toArray
  private val dataTypes = writerOptions.dataColumns.map(_.dataType).toArray
  private val wrappers = fieldOIs.zip(dataTypes).map { case (f, dt) => wrapperFor(f, dt) }
  private val outputData = new Array[Any](fieldOIs.length)

  def write(row: InternalRow): Unit = {
    //  Identify the partitionColumns and nonPartitionColumns in row
    val partitionedRow = getPartitionValues(row)
    val dataRow = getDataValues(row)

    //  Get the recordUpdater for  this partitionedRow
    val recordUpdater = getRecordUpdaterForPartition(partitionedRow)

    var i = 0
    while (i < fieldOIs.length) {
      outputData(i) = if (row.isNullAt(i)) null else wrappers(i)(dataRow.get(i, dataTypes(i)))
      i += 1
    }
    // val serializedRow = serializer.serialize(outputData, standardOI)
    // hiveWriter.write(serializedRow)
    recordUpdater.insert(writerOptions.currentWriteId, outputData)
  }

  def close(): Unit = {
    // Seems the boolean value passed into close does not matter.
    // hiveWriter.close(false)
    recordUpdaters.foreach(_._2.close(false))
  }
}

