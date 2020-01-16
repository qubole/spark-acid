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

package com.qubole.spark.hiveacid.writer.hive

import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable
import com.qubole.shaded.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter
import com.qubole.shaded.hadoop.hive.ql.exec.Utilities
import com.qubole.shaded.hadoop.hive.ql.io.{BucketCodec, HiveFileFormatUtils, RecordIdentifier, RecordUpdater, _}
import com.qubole.shaded.hadoop.hive.ql.plan.{FileSinkDesc, TableDesc}
import com.qubole.shaded.hadoop.hive.serde2.{Deserializer, SerDeUtils}
import com.qubole.shaded.hadoop.hive.serde2.Serializer
import com.qubole.shaded.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, ObjectInspectorUtils, StructObjectInspector}
import com.qubole.shaded.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import com.qubole.spark.hiveacid.{HiveAcidErrors, HiveAcidOperation}
import com.qubole.spark.hiveacid.util.Util
import com.qubole.spark.hiveacid.writer.{Writer, WriterOptions}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{JobConf, Reporter}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions.{Cast, Concat, Expression, Literal, ScalaUDF, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.hive.Hive3Inspectors
import org.apache.spark.sql.types.StringType

abstract private[writer] class HiveAcidWriter(val options: WriterOptions,
                                              val HiveAcidOptions: HiveAcidWriterOptions)
  extends Writer with Logging {

  // Takes as input partition columns returns expression to
  // create partition path. See udf `getPartitionPathString`
  // input = p1, p2, p3
  // output = Seq(Expr(p1=*), Literal(/), Expr(p2=?), Literal('/'), Expr(p3=?))
  private val partitionPathExpression: Expression = Concat(
    options.partitionColumns.zipWithIndex.flatMap { case (c, i) =>
      val partitionName = ScalaUDF(
        ExternalCatalogUtils.getPartitionPathString _,
        StringType,
        Seq(Literal(c.name), Cast(c, StringType, Option(options.timeZoneId))),
        Seq(true, true))
      if (i == 0) Seq(partitionName) else Seq(Literal(Path.SEPARATOR), partitionName)
    })

  private val getPartitionPath: InternalRow => String = {
    val proj = UnsafeProjection.create(Seq(partitionPathExpression),
      options.partitionColumns)
    row => proj(row).getString(0)
  }

  private val partitionsTouchedSet = scala.collection.mutable.HashSet[TablePartitionSpec]()
  def partitionsTouched(): Seq[TablePartitionSpec] = partitionsTouchedSet.toSeq

  // Utility functions for extracting partition and data parts of row
  //
  protected val getDataValues: InternalRow => UnsafeRow = {
    val proj = UnsafeProjection.create(options.dataColumns, options.allColumns)
    row => proj(row)
  }

  protected val getPartitionValues: InternalRow => UnsafeRow = {
    val proj = UnsafeProjection.create(options.partitionColumns,
      options.allColumns)
    row => proj(row)
  }

  protected val jobConf: JobConf = {
    val hConf = options.serializableHadoopConf.value
    new JobConf(hConf)
  }

  protected val sparkHiveRowConverter = new SparkHiveRowConverter(options, HiveAcidOptions, jobConf)

  // Cache of writers
  protected val writers: mutable.Map[(String, Int, Int), Any] = scala.collection.mutable.Map[(String, Int, Int), Any]()

  lazy protected val taskId: Int =
    Utilities.getTaskIdFromFilename(TaskContext.get.taskAttemptId().toString).toInt

  protected def getOrCreateWriter(partitionRow: InternalRow, acidBucketId: Int): Any = {

    val partitionBasePath = if (options.partitionColumns.isEmpty) {
      new Path(HiveAcidOptions.rootPath)
    } else {
      val path = getPartitionPath(partitionRow)
      partitionsTouchedSet.add(PartitioningUtils.parsePathFragment(path))
      new Path(HiveAcidOptions.rootPath, path)
    }

    writers.getOrElseUpdate((partitionBasePath.toUri.toString, taskId, acidBucketId),
      createWriter(partitionBasePath, acidBucketId))
  }

  protected def createWriter(path: Path, acidBucketId: Int): Any = {}

  // Cache structure to store row
  protected val hiveRow = new Array[Any](sparkHiveRowConverter.numFields)

  lazy protected val fileSinkConf: FileSinkDesc = {
    val fileSinkConf = HiveAcidOptions.getFileSinkDesc
    fileSinkConf.setDirName(new Path(HiveAcidOptions.rootPath))
    fileSinkConf
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
 *
 * BucketID Conundrum: In acid table single bucket cannot have multiple files.
 *
 * When performing inserts for the first time, bucketId needs to assigned such that
 * only 1 tasks writes the data to 1 bucket. Scheme used currently is to use shuffle
 * partitionID as bucketID. This scheme works even for task retry as retried task
 * overwrite already half written file, but has a problem of skew as the shuffle
 * partitionId is not based on data but on number of splits, it has bias towards
 * adding more data on lower number bucket. Ideally the data should been equally
 * distributed across multiple inserts.
 *
 * When performing delete/updates (updates are inserts + deletes) inside delete
 * delta directory bucket number in file name needs to match that of base and
 * delta file for which the delete file is being written. This is to be able
 * to prune files by name itself (penalty paid at the time of write). For handling
 * it data is repartitioned on rowId.bucketId before writing delete delta file.
 * (see [[com.qubole.spark.hiveacid.writer.TableWriter]])
 *
 * For Bucketed tables
 *
 * Invariant of however many splits that may have been created at the source the
 * data into single bucket needs to be written by single task. Hence before writing
 * the data is Same bucket cannot have multiple files
 *
 * @param options - writer options to use
 * @param HiveAcidOptions - Hive3 related writer options.
 */
private[writer] class HiveAcidFullAcidWriter(options: WriterOptions,
                                             HiveAcidOptions: HiveAcidWriterOptions)
  extends HiveAcidWriter(options, HiveAcidOptions) with Logging {

  private lazy val rowIdColNum = options.operationType match {
    case HiveAcidOperation.INSERT_INTO | HiveAcidOperation.INSERT_OVERWRITE =>
      -1
    case HiveAcidOperation.UPDATE | HiveAcidOperation.DELETE =>
      0
    case x =>
      throw new RuntimeException(s"Invalid write operation $x")
  }

  override protected def createWriter(path: Path, acidBucketId: Int): Any = {

    val tableDesc = HiveAcidOptions.getFileSinkDesc.getTableInfo

    val recordUpdater = HiveFileFormatUtils.getAcidRecordUpdater(
      jobConf,
      tableDesc,
      acidBucketId,
      HiveAcidOptions.getFileSinkDesc,
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
      case HiveAcidOperation.UPDATE => (true, true)
      case HiveAcidOperation.DELETE => (false, true)
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

  private def getBucketID(dataRow: InternalRow): Int = {
    // FIXME: Deal with bucketed table.
    val bucketedTable = false
    if (bucketedTable) {
      // getBucketIdFromCol(partitionRow)
      0
    } else {
      options.operationType match {
        case HiveAcidOperation.INSERT_INTO | HiveAcidOperation.INSERT_OVERWRITE =>
          Utilities.getTaskIdFromFilename(TaskContext.getPartitionId().toString)
            .toInt
        case HiveAcidOperation.DELETE | HiveAcidOperation.UPDATE =>
          val rowID = dataRow.get(rowIdColNum, options.rowIDSchema)
          // FIXME: Currently hard coding codec as V1 and also bucket ordinal as 1.
          BucketCodec.V1.decodeWriterId(rowID.asInstanceOf[UnsafeRow].getInt(1))
        case x =>
          throw new RuntimeException(s"Invalid write operation $x")
      }
    }
  }

  /**
   * Process an Spark InternalRow
   *
   * @param row row to be processed
   */
  def process(row: InternalRow): Unit = {
    //  Identify the partitionColumns and nonPartitionColumns in row
    val partitionColRow = getPartitionValues(row)
    val dataColRow = getDataValues(row)

    //  Get the recordWriter for  this partitionedRow
    val recordUpdater =
      getOrCreateWriter(partitionColRow, getBucketID(dataColRow)).asInstanceOf[RecordUpdater]

    val recordValue = sparkHiveRowConverter.toHiveRow(dataColRow, hiveRow)

    options.operationType match {
      case HiveAcidOperation.INSERT_INTO | HiveAcidOperation.INSERT_OVERWRITE =>
        recordUpdater.insert(options.currentWriteId, recordValue)
      case HiveAcidOperation.UPDATE =>
        recordUpdater.update(options.currentWriteId, recordValue)
      case HiveAcidOperation.DELETE =>
        recordUpdater.delete(options.currentWriteId, recordValue)
      case x =>
        throw new RuntimeException(s"Invalid write operation $x")
    }
  }

  def close(): Unit = {
    writers.foreach( x => try {
      // TODO: Seems the boolean value passed into close does not matter.
      x._2.asInstanceOf[RecordUpdater].close(false)
    }
    catch {
      case e: Exception =>
        logError("Unable to close " + x._2 + " due to: " + e.getMessage)
    })
  }
}

/**
  * This class is responsible for writing a InternalRow into a insert-only table
  * This can handle InsertInto/InsertOverwrite. This does not support Update/Delete operations
  * It has method `process` which takes 1 InternalRow and processes it based on
  * OperationType. row is expected to contain data and no row ids
  *
  * @param options writer options to use
  * @param HiveAcidOptions hive3 specific options, which is passed into underlying hive3 API
  */
private[writer] class HiveAcidInsertOnlyWriter(options: WriterOptions,
                                       HiveAcidOptions: HiveAcidWriterOptions)
  extends HiveAcidWriter(options, HiveAcidOptions) {

  override protected def createWriter(path: Path, acidBucketId: Int): Any = {
    val outputClass = sparkHiveRowConverter.serializer.getSerializedClass

    val acidOutputFormatOptions = new AcidOutputFormat.Options(jobConf)
      .writingBase(options.operationType == HiveAcidOperation.INSERT_OVERWRITE)
      .bucket(acidBucketId)
      .minimumWriteId(fileSinkConf.getTableWriteId)
      .maximumWriteId(fileSinkConf.getTableWriteId)
      .statementId(fileSinkConf.getStatementId)

    // FIXME: Hack to remove bucket prefix for Insert only table.
    var fullPathStr = AcidUtils.createFilename(path,
      acidOutputFormatOptions).toString.replace("bucket_", "")

    fullPathStr += "_" + taskId

    logDebug(s" $fullPathStr")

    HiveFileFormatUtils.getHiveRecordWriter(
      jobConf,
      sparkHiveRowConverter.tableDesc,
      outputClass,
      HiveAcidOptions.getFileSinkDesc,
      new Path(fullPathStr),
      Reporter.NULL)
  }
  /**
    * Process an Spark InternalRow
    * @param row row to be processed
    */
  override def process(row: InternalRow): Unit = {
    //  Identify the partitionColumns and nonPartitionColumns in row
    val partitionColRow = getPartitionValues(row)
    val dataColRow = getDataValues(row)

    // FIXME: Find the bucket id based on some sort hash on the data row
    val bucketId = 0

    //  Get the recordWriter for  this partitionedRow
    val writer = getOrCreateWriter(partitionColRow, bucketId)

    val recordValue =
      sparkHiveRowConverter.serialize(sparkHiveRowConverter.toHiveRow(dataColRow, hiveRow))

    options.operationType match {
      case HiveAcidOperation.INSERT_INTO | HiveAcidOperation.INSERT_OVERWRITE =>
        writer.asInstanceOf[RecordWriter].write(recordValue)
      case x =>
        throw new RuntimeException(s"Invalid write operation $x")
    }
  }

  def close(): Unit = {
    writers.foreach( x => try {
      // TODO: Seems the boolean value passed into close does not matter.
      x._2.asInstanceOf[RecordWriter].close(false)
    }
    catch {
      case e: Exception =>
        logError("Unable to close " + x._2 + " due to: " + e.getMessage)
    })
  }

}

/**
 * Utility class to convert a spark InternalRow to a row required by Hive
 * @param options - hive acid writer options
 * @param jobConf - job conf
 */
private[hive] class SparkHiveRowConverter(options: WriterOptions,
                                          HiveAcidOptions: HiveAcidWriterOptions,
                                          jobConf: JobConf) extends Hive3Inspectors {

  val tableDesc: TableDesc = HiveAcidOptions.getFileSinkDesc.getTableInfo

  // NB: Can't use tableDesc.getDeserializer as it  uses Reflection
  // internally which doesn't work because of shading. So copied its logic
  lazy val serializer: Serializer = {
    val serializer = Util.classForName(tableDesc.getSerdeClassName,
      loadShaded = true).asInstanceOf[Class[Serializer]].newInstance()
    serializer.initialize(jobConf, tableDesc.getProperties)
    serializer
  }

  lazy val deserializer: Deserializer = {
    val deserializer = Util.classForName(tableDesc.getSerdeClassName,
      loadShaded = true).asInstanceOf[Class[Deserializer]].newInstance()
    SerDeUtils.initializeSerDe(deserializer, jobConf, tableDesc.getProperties,
      null.asInstanceOf[Properties])
    deserializer
  }

  // Object Inspector Objects
  //
  private val recIdInspector = RecordIdentifier.StructInfo.oi

  private val oIWithoutRowId = ObjectInspectorUtils
      .getStandardObjectInspector(
        deserializer.getObjectInspector,
        ObjectInspectorCopyOption.JAVA)
      .asInstanceOf[StructObjectInspector]


  private val oIWithRowId = {
    val dataStructFields = asScalaIteratorConverter(
      oIWithoutRowId.getAllStructFieldRefs.iterator).asScala.toSeq

    val newFieldNameSeq = Seq("rowId") ++ dataStructFields.map(_.getFieldName)
    val newOISeq = Seq(recIdInspector) ++
      dataStructFields.map(_.getFieldObjectInspector)
    ObjectInspectorFactory.getStandardStructObjectInspector(
      newFieldNameSeq.asJava, newOISeq.asJava
    )
  }

  val objectInspector: StructObjectInspector = options.operationType match {
    case HiveAcidOperation.INSERT_INTO | HiveAcidOperation.INSERT_OVERWRITE =>
      oIWithoutRowId
    case HiveAcidOperation.DELETE | HiveAcidOperation.UPDATE =>
      oIWithRowId
    case x =>
      throw new RuntimeException(s"Invalid write operation $x")
  }

  private val fieldOIs =
    objectInspector.getAllStructFieldRefs.asScala.map(_.getFieldObjectInspector).toArray

  def getObjectInspector: StructObjectInspector = objectInspector

  def numFields: Int = fieldOIs.length

  def serialize(hiveRow: Array[Any]): Writable = {
    serializer.serialize(hiveRow, objectInspector.asInstanceOf[ObjectInspector])
  }

  def toHiveRow(sparkRow: InternalRow, hiveRow: Array[Any]): Array[Any] = {
    val dataTypes = options.dataColumns.map(_.dataType).toArray
    val wrappers = fieldOIs.zip(dataTypes).map { case (f, dt) => wrapperFor(f, dt) }

    var i = 0
    while (i < fieldOIs.length) {
      hiveRow(i) = if (sparkRow.isNullAt(i)) null else wrappers(i)(sparkRow.get(i, dataTypes(i)))
      i += 1
    }
    hiveRow
  }
}
