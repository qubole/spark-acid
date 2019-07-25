/*
 * Copyright 2019 Qubole, Inc.  All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the “License”); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qubole.spark.datasources.hiveacid.rdd

/*
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

import java.util
import java.util.Properties

import com.qubole.shaded.hadoop.hive.metastore.api.FieldSchema
import com.qubole.shaded.hadoop.hive.metastore.api.hive_metastoreConstants._
import com.qubole.shaded.hadoop.hive.metastore.utils.MetaStoreUtils.{getColumnNamesFromFieldSchema, getColumnTypesFromFieldSchema}
import com.qubole.shaded.hadoop.hive.ql.exec.Utilities
import com.qubole.shaded.hadoop.hive.ql.metadata.{Partition => HiveJarPartition, Table => HiveTable}
import com.qubole.shaded.hadoop.hive.ql.plan.TableDesc
import com.qubole.shaded.hadoop.hive.serde2.Deserializer
import com.qubole.shaded.hadoop.hive.serde2.objectinspector.primitive._
import com.qubole.shaded.hadoop.hive.serde2.objectinspector.{ObjectInspectorConverters, StructObjectInspector}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}
import com.qubole.shaded.hadoop.hive.ql.io.AcidUtils
import com.qubole.spark.datasources.hiveacid.HiveAcidState
import com.qubole.spark.datasources.hiveacid.util.{EmptyRDD, SerializableConfiguration, Util}
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
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

/**
  * A trait for subclasses that handle table scans.
  */
sealed trait TableReader {
  def makeRDDForTable(hiveTable: HiveTable): RDD[InternalRow]

  def makeRDDForPartitionedTable(partitions: Seq[HiveJarPartition]): RDD[InternalRow]
}


/**
  * Helper class for scanning tables stored in Hadoop - e.g., to read Hive tables that reside in the
  * data warehouse directory.
  */
class HiveTableReader(
                         @transient private val attributes: Seq[Attribute],
                         @transient private val partitionKeys: Seq[Attribute],
                         @transient private val tableDesc: TableDesc,
                         @transient private val sparkSession: SparkSession,
                         @transient private val acidState: HiveAcidState,
                         hadoopConf: Configuration)
  extends TableReader with CastSupport with Logging {

  private val _minSplitsPerRDD = if (sparkSession.sparkContext.isLocal) {
    0 // will splitted based on block by default.
  } else {
    math.max(hadoopConf.getInt("mapreduce.job.maps", 1),
      sparkSession.sparkContext.defaultMinPartitions)
  }

  SparkHadoopUtil.get.appendS3AndSparkHadoopConfigurations(
    sparkSession.sparkContext.getConf, hadoopConf)

  private val _broadcastedHadoopConf =
    sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

  override def conf: SQLConf = sparkSession.sessionState.conf

  override def makeRDDForTable(hiveTable: HiveTable): RDD[InternalRow] =
    makeRDDForTable(
      hiveTable,
      Util.classForName(tableDesc.getSerdeClassName,
        true).asInstanceOf[Class[Deserializer]]
    )

  /**
    * Creates a Hadoop RDD to read data from the target table's data directory. Returns a transformed
    * RDD that contains deserialized rows.
    *
    * @param hiveTable Hive metadata for the table being scanned.
    * @param deserializerClass Class of the SerDe used to deserialize Writables read from Hadoop.
    */
  def makeRDDForTable(
                       hiveTable: HiveTable,
                       deserializerClass: Class[_ <: Deserializer]): RDD[InternalRow] = {

    assert(!hiveTable.isPartitioned,
      "makeRDDForTable() cannot be called on a partitioned table, since input formats may " +
        "differ across partitions. Use makeRDDForPartitionedTable() instead.")

    // Create local references to member variables, so that the entire `this` object won't be
    // serialized in the closure below.
    val localTableDesc = tableDesc
    val broadcastedHadoopConf = _broadcastedHadoopConf

    val tablePath = hiveTable.getPath

    // logDebug("Table input: %s".format(tablePath))
    val ifcName = hiveTable.getInputFormatClass.getName
    val ifc = Util.classForName(ifcName, true).asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
    val hiveRDD = createRddForTable(localTableDesc, hiveTable.getSd.getCols,
      hiveTable.getParameters, tablePath.toString, true, ifc)

    val attrsWithIndex = attributes.zipWithIndex
    val mutableRow = new SpecificInternalRow(attributes.map(_.dataType))

    val deserializedHiveRDD = hiveRDD.mapPartitions { iter =>
      val hconf = broadcastedHadoopConf.value.value
      val deserializer = deserializerClass.newInstance()
      deserializer.initialize(hconf, localTableDesc.getProperties)
      HiveTableReader.fillObject(iter, deserializer, attrsWithIndex, mutableRow, deserializer)
    }

    new AcidLockUnionRDD[InternalRow](sparkSession.sparkContext, Seq(deserializedHiveRDD),
      Seq(), acidState)
  }

  override def makeRDDForPartitionedTable(partitions: Seq[HiveJarPartition]): RDD[InternalRow] = {
    val partitionToDeserializer = partitions.map{
      part =>
        val deserializerClassName = part.getTPartition.getSd.getSerdeInfo.getSerializationLib
        val deserializer =  Util.classForName(deserializerClassName, true)
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
                                  partitionToDeserializer: Map[HiveJarPartition, Class[_ <: Deserializer]],
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
      val mutableRow = new SpecificInternalRow(attributes.map(_.dataType))

      // Splits all attributes into two groups, partition key attributes and those that are not.
      // Attached indices indicate the position of each attribute in the output schema.
      val (partitionKeyAttrs, nonPartitionKeyAttrs) =
      attributes.zipWithIndex.partition { case (attr, _) =>
        partitionKeys.contains(attr)
      }

      def fillPartitionKeys(rawPartValues: Array[String], row: InternalRow): Unit = {
        partitionKeyAttrs.foreach { case (attr, ordinal) =>
          val partOrdinal = partitionKeys.indexOf(attr)
          row(ordinal) = cast(Literal(rawPartValues(partOrdinal)), attr.dataType).eval(null)
        }
      }

      // Fill all partition keys to the given MutableRow object
      fillPartitionKeys(partValues, mutableRow)

      val tableProperties = tableDesc.getProperties

      // Create local references so that the outer object isn't serialized.
      val localTableDesc = tableDesc
      createRddForTable(localTableDesc,  partition.getCols, partition.getTable.getParameters, inputPathStr, false,
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
        val tableSerDe = Util.classForName(tableSerDeClassName, true).newInstance().asInstanceOf[Deserializer]
        tableSerDe.initialize(hconf, localTableDesc.getProperties)

        // fill the non partition key attributes
        HiveTableReader.fillObject(iter, deserializer, nonPartitionKeyAttrs,
          mutableRow, tableSerDe)
      }
    }.toSeq

    // Even if we don't use any partitions, we still need an empty RDD
    if (hivePartitionRDDs.size == 0) {
      new EmptyRDD[InternalRow](sparkSession.sparkContext)
    } else {
      new AcidLockUnionRDD[InternalRow](hivePartitionRDDs(0).context, hivePartitionRDDs,
        partitionStrings, acidState)
    }
  }

  /**
    * If `filterOpt` is defined, then it will be used to filter files from `path`. These files are
    * returned in a single, comma-separated string.
    */
  private def applyFilterIfNeeded(path: Path, filterOpt: Option[PathFilter]): String = {
    filterOpt match {
      case Some(filter) =>
        val fs = path.getFileSystem(hadoopConf)
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
                               ): RDD[Writable] = {

    val colNames = getColumnNamesFromFieldSchema(cols)
    val colTypes = getColumnTypesFromFieldSchema(cols)
    val initializeJobConfFunc = HiveTableReader.initializeLocalJobConfFunc(path, tableDesc, tableParameters,
      colNames, colTypes) _
    val rdd = new Hive3RDD(
      sparkSession.sparkContext,
      acidState,
      _broadcastedHadoopConf.asInstanceOf[Broadcast[SerializableConfiguration]],
      Some(initializeJobConfFunc),
      inputFormatClass,
      classOf[Writable],
      classOf[Writable],
      _minSplitsPerRDD)

    // Only take the value (skip the key) because Hive works only with values.
    rdd.map(_._2)
  }
}

object HiveTableUtil {

  // copied from PlanUtils.configureJobPropertiesForStorageHandler(tableDesc)
  // that calls Hive.get() which tries to access metastore, but it's not valid in runtime
  // it would be fixed in next version of hive but till then, we should use this instead
  def configureJobPropertiesForStorageHandler(
                                               tableDesc: TableDesc, conf: Configuration, input: Boolean) {
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
}

object HiveTableReader extends Hive3Inspectors with Logging {
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
      HiveTableUtil.configureJobPropertiesForStorageHandler(tableDesc, jobConf, true)
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
    * Transform all given raw `Writable`s into `Row`s.
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
                  iterator: Iterator[Writable],
                  rawDeser: Deserializer,
                  nonPartitionKeyAttrs: Seq[(Attribute, Int)],
                  mutableRow: InternalRow,
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
      val raw = converter.convert(rawDeser.deserialize(value))
      var i = 0
      val length = fieldRefs.length
      while (i < length) {
        val fieldValue = soi.getStructFieldData(raw, fieldRefs(i))
        if (fieldValue == null) {
          mutableRow.setNullAt(fieldOrdinals(i))
        } else {
          unwrappers(i)(fieldValue, mutableRow, fieldOrdinals(i))
        }
        i += 1
      }

      mutableRow: InternalRow
    }
  }
}
