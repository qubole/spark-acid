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

package com.qubole.spark.datasources.hiveacid.reader

import java.util
import java.util.Properties

import scala.collection.JavaConverters._

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.qubole.shaded.hadoop.hive.conf.HiveConf.ConfVars
import com.qubole.shaded.hadoop.hive.metastore.api.FieldSchema
import com.qubole.shaded.hadoop.hive.metastore.api.hive_metastoreConstants._
import com.qubole.shaded.hadoop.hive.metastore.utils.MetaStoreUtils.{getColumnNamesFromFieldSchema, getColumnTypesFromFieldSchema}
import com.qubole.shaded.hadoop.hive.ql.exec.Utilities
import com.qubole.shaded.hadoop.hive.ql.io.{AcidUtils, RecordIdentifier}
import com.qubole.shaded.hadoop.hive.ql.metadata.{Partition => HiveJarPartition, Table => HiveTable}
import com.qubole.shaded.hadoop.hive.ql.plan.TableDesc
import com.qubole.shaded.hadoop.hive.serde2.Deserializer
import com.qubole.shaded.hadoop.hive.serde2.objectinspector.{ObjectInspectorConverters, StructObjectInspector}
import com.qubole.shaded.hadoop.hive.serde2.objectinspector.primitive._
import com.qubole.spark.datasources.hiveacid.HiveAcidMetadata
import com.qubole.spark.datasources.hiveacid.transaction.HiveAcidTxn
import com.qubole.spark.datasources.hiveacid.util._
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
 * Helper class for scanning tables stored in Hadoop - e.g., to read
 * Hive tables that reside in the data warehouse directory.
 */
private[reader] class Hive3Reader(@transient private val sparkSession: SparkSession,
                                  @transient private val txn: HiveAcidTxn,
                                  @transient private val readerOptions: ReaderOptions,
                                  @transient private val hive3Options: Hive3ReaderOptions)

  extends CastSupport with Logging {

  private val _minSplitsPerRDD = if (sparkSession.sparkContext.isLocal) {
    0 // will splitted based on block by default.
  } else {
    math.max(readerOptions.hadoopConf.getInt("mapreduce.job.maps", 1),
      sparkSession.sparkContext.defaultMinPartitions)
  }

  SparkHadoopUtil.get.appendS3AndSparkHadoopConfigurations(
    sparkSession.sparkContext.getConf, readerOptions.hadoopConf)

  private val _broadcastedHadoopConf =
    sparkSession.sparkContext.broadcast(new SerializableConfiguration(readerOptions.hadoopConf))

  override def conf: SQLConf = sparkSession.sessionState.conf

  def makeRDDForTable(hiveAcidMetadata: HiveAcidMetadata): RDD[InternalRow] = {
    val hiveTable = hiveAcidMetadata.hTable

    // Push Down Predicate
    if (readerOptions.readConf.predicatePushdownEnabled) {
      setPushDownFiltersInHadoopConf(readerOptions.hadoopConf,
        hiveAcidMetadata,
        readerOptions.dataFilters)
    }

    // Set Required column.
    setRequiredColumnsInHadoopConf(readerOptions.hadoopConf,
      hiveAcidMetadata,
      readerOptions.requiredNonPartitionedColumns)

    logDebug(s"sarg.pushdown: " +
      s"${readerOptions.hadoopConf.get("sarg.pushdown")}," +
      s"hive.io.file.readcolumn.names: " +
      s"${readerOptions.hadoopConf.get("hive.io.file.readcolumn.names")}, " +
      s"hive.io.file.readcolumn.ids: " +
      s"${readerOptions.hadoopConf.get("hive.io.file.readcolumn.ids")}")

    makeRDDForTable(
      hiveTable,
      Util.classForName(hive3Options.tableDesc.getSerdeClassName,
        true).asInstanceOf[Class[Deserializer]]
    )
  }

  /**
    * Creates a Hadoop RDD to read data from the target table's data directory.
    * Returns a transformed RDD that contains deserialized rows.
    *
    * @param hiveTable Hive metadata for the table being scanned.
    * @param deserializerClass Class of the SerDe used to deserialize Writables read from Hadoop.
    */
  private def makeRDDForTable(hiveTable: HiveTable,
                      deserializerClass: Class[_ <: Deserializer]): RDD[InternalRow] = {

    assert(!hiveTable.isPartitioned,
      "makeRDDForTable() cannot be called on a partitioned table, since input formats may " +
        "differ across partitions. Use makeRDDForPartitionedTable() instead.")

    // Create local references to member variables, so that the entire `this` object won't be
    // serialized in the closure below.
    val localTableDesc = hive3Options.tableDesc
    val broadcastedHadoopConf = _broadcastedHadoopConf
    val tablePath = hiveTable.getPath

    // logDebug("Table input: %s".format(tablePath))
    val ifcName = hiveTable.getInputFormatClass.getName
    val ifc = Util.classForName(ifcName, true)
      .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
    val hiveRDD = createRddForTable(localTableDesc, hiveTable.getSd.getCols,
      hiveTable.getParameters, tablePath.toString, true, ifc)

    val attrsWithIndex = readerOptions.requiredAttributes.zipWithIndex
    val localRowIdSchema: Option[StructType] = hive3Options.rowIdSchema
    val outputRowDataTypes = (localRowIdSchema match {
      case Some(schema) =>
        Seq(schema)
      case None =>
        Seq()
    }) ++ readerOptions.requiredAttributes.map(_.dataType)
    // val outputRowDataTypes = requiredAttributes.map(_.dataType)
    val mutableRow = new SpecificInternalRow(outputRowDataTypes)

    val mutableRowRecordIds = localRowIdSchema match {
      case Some(schema) => Some(new SpecificInternalRow(schema.fields.map(_.dataType)))
      case None => None
    }

    val deserializedHiveRDD = hiveRDD.mapPartitions { iter =>
      val hconf = broadcastedHadoopConf.value.value
      val deserializer = deserializerClass.newInstance()
      deserializer.initialize(hconf, localTableDesc.getProperties)
      Hive3Reader.fillObject(iter, deserializer, attrsWithIndex, mutableRow,
        mutableRowRecordIds,
        deserializer)
    }

    new Hive3UnionRDD[InternalRow](sparkSession.sparkContext, Seq(deserializedHiveRDD),
      Seq(), txn)
  }

  def makeRDDForPartitionedTable(hiveAcidMetadata: HiveAcidMetadata,
                                 partitionFilters: Seq[Filter]): RDD[InternalRow] = {
    val partitions = hiveAcidMetadata.getRawPartitions(
                        HiveSparkConversionUtil.sparkToHiveFilters(partitionFilters),
                        readerOptions.readConf.metastorePartitionPruningEnabled)

    val partitionToDeserializer = partitions.map{
      part =>
        val deserializerClassName = part.getTPartition.getSd.getSerdeInfo.getSerializationLib
        val deserializer = Util.classForName(deserializerClassName, true)
          .asInstanceOf[Class[Deserializer]]
        (part, deserializer)
    }.toMap
    makeRDDForPartitionedTable(partitionToDeserializer, filterOpt = None)
  }

  /**
    * Create a Hive3RDD for every partition key specified in the query.
    *
    * @param partitionToDeserializer Mapping from a Hive Partition metadata object to the SerDe
    *     class to use to deserialize input Writables from the corresponding partition.
    * @param filterOpt If defined, then the filter is used to reject files contained in the data
    *     subdirectory of each partition being read. If None, then all files are accepted.
    */
  def makeRDDForPartitionedTable(
                                  partitionToDeserializer: Map[HiveJarPartition,
                                    Class[_ <: Deserializer]],
                                  filterOpt: Option[PathFilter]): RDD[InternalRow] = {

    val partitionStrings = partitionToDeserializer.map { case (partition, _) =>
      //      val partKeysFieldSchema = partition.getTable.getPartitionKeys.asScala
      //      partKeysFieldSchema.map(_.getName).mkString("/")
      partition.getName
    }.toSeq

    val hivePartitionRDDs = partitionToDeserializer.map { case (partition, partDeserializer) =>
      val partProps = partition.getMetadataFromPartitionSchema
      val partPath = partition.getDataLocation
      val inputPathStr = applyFilterIfNeeded(partPath, filterOpt)
      val ifcString = partition.getTPartition.getSd.getInputFormat
      val ifc = Util.classForName(ifcString, true)
        .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
      // Get partition field info
      val partSpec = partition.getSpec
      val partCols = partition.getTable.getPartitionKeys.asScala.map(_.getName).toSeq
      // 'partValues[i]' contains the value for the partitioning column at 'partCols[i]'.
      val partValues = if (partSpec == null) {
        Array.fill(partCols.size)(new String)
      } else {
        partCols.map(col => new String(partSpec.get(col))).toArray
      }

      val broadcastedHiveConf = _broadcastedHadoopConf
      val localDeserializer = partDeserializer

      val localRowIdSchema: Option[StructType] = hive3Options.rowIdSchema
      val outputRowDataTypes = (localRowIdSchema match {
        case Some(schema) =>
          Seq(schema)
        case None =>
          Seq()
      }) ++ readerOptions.requiredAttributes.map(_.dataType)
      val mutableRow = new SpecificInternalRow(outputRowDataTypes)
      val mutableRowRecordIds = localRowIdSchema match {
        case Some(schema) => Some(new SpecificInternalRow(schema.fields.map(_.dataType)))
        case None => None
      }

      // Splits all attributes into two groups, partition key attributes and those that are not.
      // Attached indices indicate the position of each attribute in the output schema.
      val (partitionKeyAttrs, nonPartitionKeyAttrs) =
      readerOptions.requiredAttributes.zipWithIndex.partition { case (attr, _) =>
        readerOptions.partitionAttributes.contains(attr)
      }

      def fillPartitionKeys(rawPartValues: Array[String], row: InternalRow): Unit = {
        val offset = localRowIdSchema match {
          case Some(schema) =>
            1
          case None =>
            0
        }
        partitionKeyAttrs.foreach { case (attr, ordinal) =>
          val partOrdinal = readerOptions.partitionAttributes.indexOf(attr)
          row(offset + ordinal) = cast(
            Literal(rawPartValues(partOrdinal)), attr.dataType).eval(null)
        }
      }

      // Fill all partition keys to the given MutableRow object
      fillPartitionKeys(partValues, mutableRow)

      val tableProperties = hive3Options.tableDesc.getProperties

      // Create local references so that the outer object isn't serialized.
      val localTableDesc = hive3Options.tableDesc
      createRddForTable(localTableDesc, partition.getCols,
        partition.getTable.getParameters, inputPathStr, false,
        ifc).mapPartitions { iter =>
        val hconf = broadcastedHiveConf.value.value
        val deserializer = localDeserializer.newInstance()
        // SPARK-13709: For SerDes like AvroSerDe, some essential information (e.g. Avro schema
        // information) may be defined in table properties. Here we should merge table properties
        // and partition properties before initializing the deserializer. Note that partition
        // properties take a higher priority here. For example, a partition may have a different
        // SerDe as the one defined in table properties.
        val props = new Properties(tableProperties)
        partProps.asScala.foreach {
          case (key, value) => props.setProperty(key, value)
        }
        deserializer.initialize(hconf, props)
        // get the table deserializer
        val tableSerDeClassName = localTableDesc.getSerdeClassName
        val tableSerDe = Util.classForName(tableSerDeClassName,
          true).newInstance().asInstanceOf[Deserializer]
        tableSerDe.initialize(hconf, localTableDesc.getProperties)

        // fill the non partition key attributes
        Hive3Reader.fillObject(iter, deserializer, nonPartitionKeyAttrs,
          mutableRow,
          mutableRowRecordIds,
          tableSerDe)
      }
    }.toSeq

    // Even if we don't use any partitions, we still need an empty RDD
    if (hivePartitionRDDs.isEmpty) {
      new EmptyRDD[InternalRow](sparkSession.sparkContext)
    } else {
      new Hive3UnionRDD[InternalRow](hivePartitionRDDs(0).context, hivePartitionRDDs,
        partitionStrings, txn)
    }
  }

  /**
    * If `filterOpt` is defined, then it will be used to filter files from `path`. These files are
    * returned in a single, comma-separated string.
    */
  private def applyFilterIfNeeded(path: Path, filterOpt: Option[PathFilter]): String = {
    filterOpt match {
      case Some(filter) =>
        val fs = path.getFileSystem(readerOptions.hadoopConf)
        val filteredFiles = fs.listStatus(path, filter).map(_.getPath.toString)
        filteredFiles.mkString(",")
      case None => path.toString
    }
  }

  /**
    * Creates a Hive3RDD based on the broadcasted HiveConf and other job properties that will be
    * applied locally on each slave.
    */
  private def createRddForTable(tableDesc: TableDesc,
                                cols: util.List[FieldSchema],
                                tableParameters: util.Map[String, String],
                                path: String,
                                acquireLocks: Boolean,
                                inputFormatClass: Class[InputFormat[Writable, Writable]]
                               ): RDD[(RecordIdentifier, Writable)] = {

    val colNames = getColumnNamesFromFieldSchema(cols)
    val colTypes = getColumnTypesFromFieldSchema(cols)
    val initializeJobConfFunc = Hive3Reader.initializeLocalJobConfFunc(
      path, tableDesc, tableParameters,
      colNames, colTypes) _
    val rdd = new Hive3RDD(
      sparkSession.sparkContext,
      txn,
      _broadcastedHadoopConf.asInstanceOf[Broadcast[SerializableConfiguration]],
      Some(initializeJobConfFunc),
      inputFormatClass,
      classOf[Writable],
      classOf[Writable],
      _minSplitsPerRDD)

    rdd
  }

  private def setRequiredColumnsInHadoopConf(conf: Configuration,
                                     acidTableMetadata: HiveAcidMetadata,
                                     requiredColumns: Seq[String]): Unit = {
    val dataCols: Seq[String] = acidTableMetadata.dataSchema.fields.map(_.name)
    val requiredColumnIndexes = requiredColumns.map(a => dataCols.indexOf(a): Integer)
    val (sortedIDs, sortedNames) = requiredColumnIndexes.zip(requiredColumns).sorted.unzip
    conf.set(ColumnProjectionUtils.READ_ALL_COLUMNS, "false")
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, sortedNames.mkString(","))
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, sortedIDs.mkString(","))
  }

  private def setPushDownFiltersInHadoopConf(conf: Configuration,
                                     acidTableMetadata: HiveAcidMetadata,
                                     dataFilters: Array[Filter]): Unit = {
    Hive3SearchArgument.build(acidTableMetadata.dataSchema, dataFilters).foreach { f =>
      def toKryo(obj: com.qubole.shaded.hadoop.hive.ql.io.sarg.SearchArgument): String = {
        val out = new Output(4 * 1024, 10 * 1024 * 1024)
        new Kryo().writeObject(out, obj)
        out.close()
        return Base64.encodeBase64String(out.toBytes)
      }

      logDebug(s"searchArgument: ${f}")
      conf.set("sarg.pushdown", toKryo(f))
      conf.setBoolean(ConfVars.HIVEOPTINDEXFILTER.varname, true)
    }
  }
}

private[reader] object Hive3Reader extends Hive3Inspectors with Logging {

  // copied from PlanUtils.configureJobPropertiesForStorageHandler(tableDesc)
  // that calls Hive.get() which tries to access metastore, but it's not valid in runtime
  // it would be fixed in next version of hive but till then, we should use this instead
  private def configureJobPropertiesForStorageHandler(tableDesc: TableDesc,
                                                      conf: Configuration, input: Boolean) {
    val property = tableDesc.getProperties.getProperty(META_TABLE_STORAGE)
    val storageHandler =
      com.qubole.shaded.hadoop.hive.ql.metadata.HiveUtils.getStorageHandler(conf, property)
    if (storageHandler != null) {
      val jobProperties = new java.util.LinkedHashMap[String, String]
      if (input) {
        storageHandler.configureInputJobProperties(tableDesc, jobProperties)
      } else {
        storageHandler.configureOutputJobProperties(tableDesc, jobProperties)
      }
      if (!jobProperties.isEmpty) {
        tableDesc.setJobProperties(jobProperties)
      }
    }
  }

  /**
    * Curried. After given an argument for 'path', the resulting JobConf => Unit closure is used to
    * instantiate a Hive3RDD.
    */
  def initializeLocalJobConfFunc(path: String, tableDesc: TableDesc,
                                 tableParameters: util.Map[String, String],
                                 schemaColNames: String,
                                 schemaColTypes: String)(jobConf: JobConf) {
    FileInputFormat.setInputPaths(jobConf, Seq[Path](new Path(path)): _*)
    if (tableDesc != null) {
      configureJobPropertiesForStorageHandler(tableDesc, jobConf, true)
      Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
    }
    val bufferSize = System.getProperty("spark.buffer.size", "65536")
    jobConf.set("io.file.buffer.size", bufferSize)
    jobConf.set("schema.evolution.columns", schemaColNames)
    jobConf.set("schema.evolution.columns.types", schemaColTypes)

    // Set HiveACID related properties in jobConf
    val isTranscationalTable = AcidUtils.isTransactionalTable(tableParameters)
    val acidProps = if (isTranscationalTable) {
      AcidUtils.getAcidOperationalProperties(tableParameters)
    } else {
      null
    }
    AcidUtils.setAcidOperationalProperties(jobConf, isTranscationalTable, acidProps)
  }

  /**
    * Transform all given raw `(RowIdentifier, Writable)`s into `InternalRow`s.
    *
    * @param iterator Iterator of all `Writable`s to be transformed
    * @param rawDeser The `Deserializer` associated with the input `Writable`
    * @param nonPartitionKeyAttrs Attributes that should be filled together with their corresponding
    *                             positions in the output schema
    * @param mutableRow A reusable `MutableRow` that should be filled
    * @param tableDeser Table Deserializer
    * @return An `Iterator[Row]` transformed from `iterator`
    */
  def fillObject(
                  iterator: Iterator[(RecordIdentifier, Writable)],
                  rawDeser: Deserializer,
                  nonPartitionKeyAttrs: Seq[(Attribute, Int)],
                  mutableRow: InternalRow,
                  mutableRowRecordId: Option[InternalRow],
                  tableDeser: Deserializer): Iterator[InternalRow] = {

    val soi = if (rawDeser.getObjectInspector.equals(tableDeser.getObjectInspector)) {
      rawDeser.getObjectInspector.asInstanceOf[StructObjectInspector]
    } else {
      ObjectInspectorConverters.getConvertedOI(
        rawDeser.getObjectInspector,
        tableDeser.getObjectInspector).asInstanceOf[StructObjectInspector]
    }

    logDebug(soi.toString)

    val (fieldRefs, fieldOrdinals) = nonPartitionKeyAttrs.map { case (attr, ordinal) =>
      soi.getStructFieldRef(attr.name) -> ordinal
    }.toArray.unzip

    // Builds specific unwrappers ahead of time according to object inspector
    // types to avoid pattern matching and branching costs per row.
    val unwrappers: Seq[(Any, InternalRow, Int) => Unit] = fieldRefs.map {
      x =>
        val y = x.getFieldObjectInspector
        y match {
          case oi: BooleanObjectInspector =>
            (value: Any, row: InternalRow, ordinal: Int) => row.setBoolean(ordinal, oi.get(value))
          case oi: ByteObjectInspector =>
            (value: Any, row: InternalRow, ordinal: Int) => row.setByte(ordinal, oi.get(value))
          case oi: ShortObjectInspector =>
            (value: Any, row: InternalRow, ordinal: Int) => row.setShort(ordinal, oi.get(value))
          case oi: IntObjectInspector =>
            (value: Any, row: InternalRow, ordinal: Int) => row.setInt(ordinal, oi.get(value))
          case oi: LongObjectInspector =>
            (value: Any, row: InternalRow, ordinal: Int) => row.setLong(ordinal, oi.get(value))
          case oi: FloatObjectInspector =>
            (value: Any, row: InternalRow, ordinal: Int) => row.setFloat(ordinal, oi.get(value))
          case oi: DoubleObjectInspector =>
            (value: Any, row: InternalRow, ordinal: Int) => row.setDouble(ordinal, oi.get(value))
          case oi: HiveVarcharObjectInspector =>
            (value: Any, row: InternalRow, ordinal: Int) =>
              row.update(ordinal, UTF8String.fromString(oi.getPrimitiveJavaObject(value).getValue))
          case oi: HiveCharObjectInspector =>
            (value: Any, row: InternalRow, ordinal: Int) =>
              row.update(ordinal, UTF8String.fromString(oi.getPrimitiveJavaObject(value).getValue))
          case oi: HiveDecimalObjectInspector =>
            (value: Any, row: InternalRow, ordinal: Int) =>
              row.update(ordinal, toCatalystDecimal(oi, value))
          case oi: TimestampObjectInspector =>
            (value: Any, row: InternalRow, ordinal: Int) =>
              row.setLong(ordinal, DateTimeUtils.fromJavaTimestamp(
                oi.getPrimitiveJavaObject(value).toSqlTimestamp))
          case oi: DateObjectInspector =>
            (value: Any, row: InternalRow, ordinal: Int) =>
              val y = oi.getPrimitiveWritableObject(value).get().toEpochMilli
              row.setInt(ordinal, DateTimeUtils.fromJavaDate(new java.sql.Date(y)))
          case oi: BinaryObjectInspector =>
            (value: Any, row: InternalRow, ordinal: Int) =>
              row.update(ordinal, oi.getPrimitiveJavaObject(value))
          case oi =>
            val unwrapper = unwrapperFor(oi)
            (value: Any, row: InternalRow, ordinal: Int) => row(ordinal) = unwrapper(value)
        }
    }

    val converter = ObjectInspectorConverters.getConverter(rawDeser.getObjectInspector, soi)

    // Map each tuple to a row object
    iterator.map { value =>
      val dataStartIndex = mutableRowRecordId match {
        case Some(record) =>
          val recordIdentifier = value._1
          record.setLong(0, recordIdentifier.getWriteId)
          record.setInt(1, recordIdentifier.getBucketProperty)
          record.setLong(2, recordIdentifier.getRowId)
          mutableRow.update(0, record)
          1
        case None =>
          0
      }

      val raw = converter.convert(rawDeser.deserialize(value._2))
      var i = 0
      val length = fieldRefs.length
      while (i < length) {
        val fieldValue = soi.getStructFieldData(raw, fieldRefs(i))
        if (fieldValue == null) {
          mutableRow.setNullAt(fieldOrdinals(i) + dataStartIndex)
        } else {
          unwrappers(i)(fieldValue, mutableRow, fieldOrdinals(i) + dataStartIndex)
        }
        i += 1
      }

      mutableRow: InternalRow
    }
  }


}
