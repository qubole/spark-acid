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

package com.qubole.spark.hiveacid.reader.hive

import java.util
import java.util.{List, Properties}

import scala.collection.JavaConverters._
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.qubole.shaded.hadoop.hive.conf.HiveConf.ConfVars
import com.qubole.shaded.hadoop.hive.common.ValidWriteIdList
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
import com.qubole.spark.hiveacid.HiveAcidErrors
import com.qubole.spark.hiveacid.hive.HiveAcidMetadata
import com.qubole.spark.hiveacid.hive.HiveConverter
import com.qubole.spark.hiveacid.reader.{Reader, ReaderOptions, ReaderPartition}
import com.qubole.spark.hiveacid.rdd._
import com.qubole.spark.hiveacid.util._
import com.qubole.spark.hiveacid.reader.v2.HiveAcidInputPartitionV2
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
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.hive.{Hive3Inspectors, HiveAcidUtils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.types._

/**
 * Helper class for scanning tables stored in Hadoop - e.g., to read
 * Hive tables that reside in the data warehouse directory.
 * @param sparkSession - spark session
 * @param readerOptions - reader options for creating RDD
 * @param hiveAcidOptions - hive related reader options for creating RDD
 * @param validWriteIds - validWriteIds
 * @param partitions - The list of partitions to be scanned. Valid only for partitioned table.
 */
private[reader] class HiveAcidReader(sparkSession: SparkSession,
                                     readerOptions: ReaderOptions,
                                     hiveAcidOptions: HiveAcidReaderOptions,
                                     validWriteIds: ValidWriteIdList,
                                     partitions: Seq[ReaderPartition])

extends CastSupport with Reader with Logging {

  private val _minSplitsPerRDD = if (sparkSession.sparkContext.isLocal) {
    0 // will be split based on block by default.
  } else {
    math.max(readerOptions.hadoopConf.getInt("mapreduce.job.maps", 1),
      sparkSession.sparkContext.defaultMinPartitions)
  }

  SparkHadoopUtil.get.appendS3AndSparkHadoopConfigurations(
    sparkSession.sparkContext.getConf, readerOptions.hadoopConf)

  if (readerOptions.readConf.predicatePushdownEnabled) {
    setPushDownFiltersInHadoopConf(readerOptions.hadoopConf, hiveAcidOptions.dataSchema,
      readerOptions.dataFilters)
  }

  // Set Required column.
  setRequiredColumnsInHadoopConf(readerOptions.hadoopConf,
    hiveAcidOptions.dataSchema,
    readerOptions.requiredNonPartitionedColumns)


  // NOTE: All hadoop/hive configs should be set before broadcasting hadoopConf
  private val _broadcastedHadoopConf =
    sparkSession.sparkContext.broadcast(new SerializableConfiguration(readerOptions.hadoopConf))

  override def conf: SQLConf = sparkSession.sessionState.conf

  def makeV2ReaderForPath(hiveAcidMetadata: HiveAcidMetadata,
                          path : String,
                          fieldSchemas: util.List[FieldSchema],
                          partitionValues : InternalRow): java.util.List[InputPartition[ColumnarBatch]] = {
    setReaderOptions(hiveAcidMetadata)

    val ifcName = hiveAcidMetadata.hTable.getInputFormatClass.getName
    val inputFormatClass = Util.classForName(ifcName, loadShaded = true)
      .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]

    val colNames = getColumnNamesFromFieldSchema(fieldSchemas)
    val colTypes = getColumnTypesFromFieldSchema(fieldSchemas)
    val initializeJobConfFunc = HiveAcidReader.initializeLocalJobConfFunc(
      path, hiveAcidOptions.tableDesc,
      hiveAcidMetadata.hTable.getParameters,
      colNames, colTypes) _

    //TODO :Its a ugly hack, but avoids lots of duplicate code.
    val rdd = new HiveAcidRDD(
      sparkSession.sparkContext,
      validWriteIds,
      hiveAcidOptions.isFullAcidTable,
      _broadcastedHadoopConf.asInstanceOf[Broadcast[SerializableConfiguration]],
      Some(initializeJobConfFunc),
      inputFormatClass,
      classOf[Writable],
      classOf[Writable],
      _minSplitsPerRDD)
    val jobConf = rdd.getJobConf
    val inputSplits  = rdd.getPartitions

    val reqFields = hiveAcidMetadata.tableSchema.fields.filter(field =>
      readerOptions.requiredNonPartitionedColumns.contains(field.name))

    val partitionArray = new java.util.ArrayList[InputPartition[ColumnarBatch]]
    for (i <- 0 until inputSplits.size) {
      partitionArray.add(new HiveAcidInputPartitionV2(inputSplits(i).asInstanceOf[HiveAcidPartition],
        sparkSession.sparkContext.broadcast(new SerializableConfiguration(jobConf)),
        partitionValues, reqFields, hiveAcidMetadata.partitionSchema, hiveAcidMetadata.isFullAcidTable))
      logInfo("getPartitions : Input split: " + inputSplits(i))
    }
    partitionArray
  }

  def makeV2ReaderForTable(hiveAcidMetadata: HiveAcidMetadata): java.util.List[InputPartition[ColumnarBatch]] = {
    makeV2ReaderForPath(hiveAcidMetadata, hiveAcidMetadata.hTable.getPath.toString,
                      hiveAcidMetadata.hTable.getSd.getCols,
                      new SpecificInternalRow(hiveAcidMetadata.partitionSchema))
  }

  private def setReaderOptions(hiveAcidMetadata: HiveAcidMetadata) : Unit = {
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
  }

  /**
   * @param hiveAcidMetadata - hive acid metadata for underlying table
   * @return - Returns RDD on top of non partitioned hive acid table and list of partitionNames empty list
   *         for entire table
   */
  def makeRDDForTable(hiveAcidMetadata: HiveAcidMetadata): RDD[InternalRow] = {
      setReaderOptions(hiveAcidMetadata)
    makeRDDForTable(
      hiveAcidMetadata.hTable,
      Util.classForName(hiveAcidOptions.tableDesc.getSerdeClassName,
        loadShaded = true).asInstanceOf[Class[Deserializer]],
      hiveAcidMetadata,
      readerOptions
    )
  }

  /**
   * @param hiveAcidMetadata - hive acid metadata of underlying table
   *
   * @return - Returns RDD on top of partitioned hive acid table
   */
  def makeRDDForPartitionedTable(hiveAcidMetadata: HiveAcidMetadata): RDD[InternalRow] = {

    val partitionToDeserializer = partitions.map(p => p.ptn.asInstanceOf[HiveJarPartition]).map {
      part =>
        val deserializerClassName = part.getTPartition.getSd.getSerdeInfo.getSerializationLib
        val deserializer = Util.classForName(deserializerClassName, loadShaded = true)
          .asInstanceOf[Class[Deserializer]]
        (part, deserializer)
    }.toMap
    makeRDDForPartitionedTable(partitionToDeserializer, filterOpt = None, readerOptions)
  }

  def makeReaderForPartitionedTable(hiveAcidMetadata: HiveAcidMetadata):
                    java.util.ArrayList[InputPartition[ColumnarBatch]] = {
    val partitionToDeserializer = getPartitionToDeserializer(partitions)
    val partitionArray = new java.util.ArrayList[InputPartition[ColumnarBatch]]
    val partList = partitionToDeserializer.map { case (partition, partDeserializer) =>
      val partPath = partition.getDataLocation
      val inputPathStr = applyFilterIfNeeded(partPath, None)
      val partSpec = partition.getSpec
      val partCols = partition.getTable.getPartitionKeys.asScala.map(_.getName)

      // 'partValues[i]' contains the value for the partitioning column at 'partCols[i]'.
      val partValues = if (partSpec == null) {
        Array.fill(partCols.size)(new String)
      } else {
        partCols.map(col => new String(partSpec.get(col))).toArray
      }

      val mutableRow = new SpecificInternalRow(hiveAcidMetadata.partitionSchema)

      val partitionKeyAttrs =
      readerOptions.requiredAttributes.zipWithIndex.filter { attr =>
        readerOptions.partitionAttributes.contains(attr)
      }

      //TODO : The partition values can be filled directly using hive acid batch reader.
      def fillPartitionKeys(rawPartValues: Array[String], row: InternalRow): Unit = {
        var offset = 0
        partitionKeyAttrs.foreach { case (attr, ordinal) =>
          val partOrdinal = readerOptions.partitionAttributes.indexOf(attr)
          row(offset) = cast(
            Literal(rawPartValues(partOrdinal)), attr.dataType).eval(null)
          offset = offset + 1
        }
      }
      fillPartitionKeys(partValues, mutableRow)

      makeV2ReaderForPath(hiveAcidMetadata, inputPathStr,  partition.getTPartition.getSd.getCols, mutableRow)
    }
    for (list <- partList) partitionArray.addAll(list)
    partitionArray
  }

  private def getPartitionToDeserializer(partitions: Seq[ReaderPartition])
  : Map[HiveJarPartition, Class[_ <: Deserializer]] = {
    partitions.map(p => p.ptn.asInstanceOf[HiveJarPartition]).map {
      part =>
        val deserializerClassName = part.getTPartition.getSd.getSerdeInfo.getSerializationLib
        val deserializer = Util.classForName(deserializerClassName, loadShaded = true)
          .asInstanceOf[Class[Deserializer]]
        (part, deserializer)
    }.toMap
  }

  /**
   * Creates a Hadoop RDD to read data from the target table's data directory.
   * Returns a transformed RDD that contains deserialized rows.
   *
   * @param hiveTable Hive metadata for the table being scanned.
   * @param deserializerClass Class of the SerDe used to deserialize Writables read from Hadoop.
   */
  private def makeRDDForTable(hiveTable: HiveTable,
                              deserializerClass: Class[_ <: Deserializer],
                              hiveAcidMetadata: HiveAcidMetadata,
                              readerOptions: ReaderOptions): RDD[InternalRow] = {

    assert(!hiveTable.isPartitioned,
      "makeRDDForTable() cannot be called on a partitioned table, since input formats may " +
        "differ across partitions. Use makeRDDForPartitionedTable() instead.")

    // Create local references to member variables, so that the entire `this` object won't be
    // serialized in the closure below.
    val localTableDesc = hiveAcidOptions.tableDesc
    val tablePath = hiveTable.getPath

    val ifcName = hiveTable.getInputFormatClass.getName
    val ifc = Util.classForName(ifcName, loadShaded = true)
      .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
    val hiveRDD = createRddForTable(localTableDesc, hiveTable.getSd.getCols,
      hiveTable.getParameters, tablePath.toString, ifc)
    deserializeTableRdd(hiveRDD, deserializerClass)
  }

  /**
    * Utility to convert hiveRDD on non-partitioned table to Deserialized RDD which transforms
    * raw Records from hiveRDD to InternalRow
    * @param hiveRDD
    * @param deserializerClass
    * @return Deserialized RDD
    */
  private def deserializeTableRdd(hiveRDD: RDD[(RecordIdentifier, Writable)],
                                  deserializerClass: Class[_ <: Deserializer]) = {
    val localTableDesc = hiveAcidOptions.tableDesc
    val broadcastedHadoopConf = _broadcastedHadoopConf
    val attrsWithIndex = readerOptions.requiredAttributes.zipWithIndex
    val outputRowDataTypes = readerOptions.requiredAttributes.map(_.dataType)
    val mutableRow = new SpecificInternalRow(outputRowDataTypes)

    val shouldIncludeRowIds = readerOptions.readConf.includeRowIds &&
      readerOptions.requiredAttributes.exists(HiveAcidReader.isAttributeRowId(_))
    val mutableRowRecordIds = if (shouldIncludeRowIds) {
      Some(new SpecificInternalRow(HiveAcidMetadata.rowIdSchema.fields.map(_.dataType)))
    } else {
      None
    }

    // ReaderOptions cannot be serialized
    val deserializedHiveRDD = hiveRDD.mapPartitions { iter =>
      val hconf = broadcastedHadoopConf.value.value
      val deserializer = deserializerClass.newInstance()
      deserializer.initialize(hconf, localTableDesc.getProperties)
      HiveAcidReader.fillObject(iter, deserializer, attrsWithIndex, mutableRow,
        mutableRowRecordIds,
        deserializer)
    }
    new HiveAcidUnionRDD[InternalRow](sparkSession.sparkContext, Seq(deserializedHiveRDD), Seq())
  }

  /**
   * Create a HiveAcidRDD for every partition key specified in the query.
   *
   * @param partitionToDeserializer Mapping from a Hive Partition metadata object to the SerDe
   *     class to use to deserialize input Writables from the corresponding partition.
   * @param filterOpt If defined, then the filter is used to reject files contained in the data
   *     subdirectory of each partition being read. If None, then all files are accepted.
   */
  private def makeRDDForPartitionedTable(
      partitionToDeserializer: Map[HiveJarPartition, Class[_ <: Deserializer]],
      filterOpt: Option[PathFilter],
      readerOptions: ReaderOptions): RDD[InternalRow] = {

    val hivePartitionRDDSeq = partitionToDeserializer.map { case (partition, partDeserializer) =>
      val partPath = partition.getDataLocation
      val inputPathStr = applyFilterIfNeeded(partPath, filterOpt)
      val ifcString = partition.getTPartition.getSd.getInputFormat
      val ifc = Util.classForName(ifcString, loadShaded = true)
        .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]

      // Create local references so that the outer object isn't serialized.
      val localTableDesc = hiveAcidOptions.tableDesc
      val rdd = createRddForTable(localTableDesc, partition.getCols,
        partition.getTable.getParameters, inputPathStr, ifc)
      val hiveSplitInfo = rdd.asInstanceOf[HiveAcidRDD[Writable, Writable]].getHiveSplitsInfo
      (rdd, partition, partDeserializer, hiveSplitInfo)
    }.toSeq

    val hivePartitionRDDs = hivePartitionRDDSeq.map {
      case (hiveRDD, partition, partDeserializer, _)  => {
        makeRddForPartition(hiveRDD, partition, partDeserializer, readerOptions)
      }
    }

    // Even if we don't use any partitions, we still need an empty RDD
    if (hivePartitionRDDs.isEmpty) {
      new EmptyRDD[InternalRow](sparkSession.sparkContext)
    } else {
      val hiveSplitInfos = hivePartitionRDDSeq.map(_._4)
      new HiveAcidUnionRDD[InternalRow](hivePartitionRDDs.head.context, hivePartitionRDDs, hiveSplitInfos)
    }
  }

  private def makeRddForPartition(hiveRDD: RDD[(RecordIdentifier, Writable)],
                                  partition: HiveJarPartition,
                                  partDeserializer: Class[_ <: Deserializer],
                                  readerOptions: ReaderOptions) = {
    deserializePartitionRdd(hiveRDD, partition, partDeserializer)
  }

  /**
    * Convert Hive RDD for a particular partition into Deserialized RDD which transforms every Raw row
    * into InternalRow.
    *
    * @param partitionRDD
    * @param partition
    * @param partDeserializer
    * @return
    */
  private def deserializePartitionRdd(partitionRDD: RDD[(RecordIdentifier, Writable)], partition: HiveJarPartition, partDeserializer: Class[_ <: Deserializer]) = {
    // member variable cannot be used directly inside mapPartition as HiveAcidReader is not serializable.
    val broadcastedHadoopConf = _broadcastedHadoopConf
    val tableProperties = hiveAcidOptions.tableDesc.getProperties
    val partProps = partition.getMetadataFromPartitionSchema
    val localTableDesc = hiveAcidOptions.tableDesc
    val outputRowDataTypes = readerOptions.requiredAttributes.map(_.dataType)
    // Get partition field info
    val partSpec = partition.getSpec
    val partCols = partition.getTable.getPartitionKeys.asScala.map(_.getName)
    // 'partValues[i]' contains the value for the partitioning column at 'partCols[i]'.
    val partValues = if (partSpec == null) {
      Array.fill(partCols.size)(new String)
    } else {
      partCols.map(col => new String(partSpec.get(col))).toArray
    }
    val mutableRow = new SpecificInternalRow(outputRowDataTypes)
    val shouldIncludeRowIds = readerOptions.readConf.includeRowIds &&
      readerOptions.requiredAttributes.exists(HiveAcidReader.isAttributeRowId(_))
    val mutableRowRecordIds = if (shouldIncludeRowIds) {
      Some(new SpecificInternalRow(HiveAcidMetadata.rowIdSchema.fields.map(_.dataType)))
    } else {
      None
    }

    // Splits all attributes into two groups, partition key attributes and those
    // that are not. Attached indices indicate the position of each attribute in
    // the output schema.
    val (partitionKeyAttrs, nonPartitionKeyAttrs) =
    readerOptions.requiredAttributes.zipWithIndex.partition { case (attr, _) =>
      readerOptions.partitionAttributes.contains(attr)
    }

    def fillPartitionKeys(rawPartValues: Array[String], row: InternalRow, includeRowIds: Boolean): Unit = {
      partitionKeyAttrs.foreach { case (attr, ordinal) =>
        val partOrdinal = readerOptions.partitionAttributes.indexOf(attr)
        row(ordinal) = cast(
          Literal(rawPartValues(partOrdinal)), attr.dataType).eval(null)
      }
    }

    // Fill all partition keys to the given MutableRow object
    fillPartitionKeys(partValues, mutableRow, shouldIncludeRowIds)


    partitionRDD.mapPartitions { iter =>
      val hconf = broadcastedHadoopConf.value.value
      val deserializer = partDeserializer.newInstance()
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
        loadShaded = true).newInstance().asInstanceOf[Deserializer]
      tableSerDe.initialize(hconf, localTableDesc.getProperties)

      // fill the non partition key attributes
      HiveAcidReader.fillObject(iter, deserializer, nonPartitionKeyAttrs,
        mutableRow,
        mutableRowRecordIds,
        tableSerDe)
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
    * Creates a HiveAcidRDD based on the broadcasted HiveConf and other job properties that will be
    * applied locally on each slave.
    */
  private def createRddForTable(tableDesc: TableDesc,
                                cols: util.List[FieldSchema],
                                tableParameters: util.Map[String, String],
                                path: String,
                                inputFormatClass: Class[InputFormat[Writable, Writable]]
                               ): RDD[(RecordIdentifier, Writable)] = {

    val colNames = getColumnNamesFromFieldSchema(cols)
    val colTypes = getColumnTypesFromFieldSchema(cols)
    val initializeJobConfFunc = HiveAcidReader.initializeLocalJobConfFunc(
      path, tableDesc, tableParameters,
      colNames, colTypes) _
    val rdd = new HiveAcidRDD(
      sparkSession.sparkContext,
      validWriteIds,
      hiveAcidOptions.isFullAcidTable,
      _broadcastedHadoopConf.asInstanceOf[Broadcast[SerializableConfiguration]],
      Some(initializeJobConfFunc),
      inputFormatClass,
      classOf[Writable],
      classOf[Writable],
      _minSplitsPerRDD)

    rdd
  }

  private def setRequiredColumnsInHadoopConf(conf: Configuration,
                                             dataSchema: StructType,
                                     requiredColumns: Seq[String]): Unit = {
    val dataCols: Seq[String] = dataSchema.fields.map(_.name)
    val requiredColumnIndexes = requiredColumns.map(a => dataCols.indexOf(a): Integer)
    val (sortedIDs, sortedNames) = requiredColumnIndexes.zip(requiredColumns).sorted.unzip
    conf.set(ColumnProjectionUtils.READ_ALL_COLUMNS, "false")
    conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, sortedNames.mkString(","))
    conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, sortedIDs.mkString(","))
  }

  private def setPushDownFiltersInHadoopConf(conf: Configuration,
                                             dataSchema: StructType,
                                             dataFilters: Array[Filter]): Unit = {
    HiveAcidSearchArgument.build(dataSchema, dataFilters).foreach { f =>
      def toKryo(obj: com.qubole.shaded.hadoop.hive.ql.io.sarg.SearchArgument): String = {
        val out = new Output(4 * 1024, 10 * 1024 * 1024)
        new Kryo().writeObject(out, obj)
        out.close()
        Base64.encodeBase64String(out.toBytes)
      }

      logDebug(s"searchArgument: $f")
      conf.set("sarg.pushdown", toKryo(f))
      conf.setBoolean(ConfVars.HIVEOPTINDEXFILTER.varname, true)
    }
  }
}

private[reader] object HiveAcidReader extends Hive3Inspectors with Logging {

  def getPartitions(hiveAcidMetadata: HiveAcidMetadata,
                    readerOptions: ReaderOptions,
                    partitionFilters: Seq[Filter]): (Seq[ReaderPartition], Seq[String]) = {

    val partitions = if (hiveAcidMetadata.isPartitioned) {
      if (readerOptions.readConf.metastorePartitionPruningEnabled) {
        val partitionPruningFiltering = Option(HiveConverter.compileFilters(partitionFilters))
        try {
          hiveAcidMetadata.getRawPartitions(partitionPruningFiltering)
        } catch {
          // TODO: Enable pruning results returned by getting all Partitions
          case ex: com.qubole.shaded.hadoop.hive.metastore.api.MetaException => {
            logWarning("Caught Hive MetaException attempting to get partition metadata by " +
              "filter from Hive. Falling back to fetching all partition metadata and pruning them. " +
              "Filter: " + partitionPruningFiltering)
            logDebug("Exception: " + ex.getMessage, ex)
            val allPartitions = hiveAcidMetadata.getRawPartitions()
            try {
              prunePartitions(allPartitions, hiveAcidMetadata, readerOptions, partitionPruningFiltering)
            } catch {
              case e: Exception =>
                logWarning("Error while pruning the partitions. All the partitions will be returned" +
                  " that can cause Performance impact", e)
                allPartitions
            }
          }
        }
      } else {
        hiveAcidMetadata.getRawPartitions()
      }
    } else {
      Seq()
    }

    val partitionNames = partitions.map(_.getName())

    (partitions.map(p => ReaderPartition(p)), partitionNames)
  }

  /**
    * This will try to fetch all Partitions and prune them according to filter specified
    * @return prunedPartitions
    */
  private def prunePartitions(partitions: Seq[HiveJarPartition],
                              hiveAcidMetadata: HiveAcidMetadata,
                              readerOptions: ReaderOptions,
                              partitionPruningFiltering: Option[String]): Seq[HiveJarPartition] = {
    partitionPruningFiltering match {
      case Some(filter) => {
        val filterExp = functions.expr(filter).expr
        val partitionMap: Map[CatalogTablePartition, HiveJarPartition] =
          partitions.map(hp => (HiveAcidUtils.convertToCatalogTablePartition(hp), hp)).toMap
        val catalogParts = HiveAcidUtils.prunePartitionsByFilter(hiveAcidMetadata,
          partitionMap.keys.toSeq,
          Some(filterExp),
          readerOptions.sessionLocalTimeZone)
        val partCols = hiveAcidMetadata.partitionSchema.fieldNames
        catalogParts.map {
          cPart =>
            val partValues = partCols.map { hc =>
              cPart.spec.get(hc).getOrElse {
                throw new IllegalArgumentException(
                  s"Partition spec is missing a value for column '${hc}': ${cPart.spec}")
              }
            }
            partitionMap.get(cPart).getOrElse {
              throw new IllegalArgumentException(
                s"Partition values is missing from raw partitions ${partValues}"
              )
            }
        }
      }
      case _ => partitions
    }
  }

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
    * instantiate a HiveAcidRDD.
    */
  def initializeLocalJobConfFunc(path: String, tableDesc: TableDesc,
                                 tableParameters: util.Map[String, String],
                                 schemaColNames: String,
                                 schemaColTypes: String)(jobConf: JobConf) {
    FileInputFormat.setInputPaths(jobConf, Seq[Path](new Path(path)): _*)
    if (tableDesc != null) {
      configureJobPropertiesForStorageHandler(tableDesc, jobConf, input = true)
      Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
    }
    val bufferSize = System.getProperty("spark.buffer.size", "65536")
    jobConf.set("io.file.buffer.size", bufferSize)
    jobConf.set("schema.evolution.columns", schemaColNames)
    jobConf.set("schema.evolution.columns.types", schemaColTypes)

    // Set HiveACID related properties in jobConf
    val isTransactionalTable = AcidUtils.isTransactionalTable(tableParameters)
    val acidProps = if (isTransactionalTable) {
      AcidUtils.getAcidOperationalProperties(tableParameters)
    } else {
      null
    }
    AcidUtils.setAcidOperationalProperties(jobConf, isTransactionalTable, acidProps)
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

    // Note mutableRowRecordId will be None when no rowIds needs to be included in the InternalRow.
    val soi = if (rawDeser.getObjectInspector.equals(tableDeser.getObjectInspector)) {
      rawDeser.getObjectInspector.asInstanceOf[StructObjectInspector]
    } else {
      ObjectInspectorConverters.getConvertedOI(
        rawDeser.getObjectInspector,
        tableDeser.getObjectInspector).asInstanceOf[StructObjectInspector]
    }

    logDebug(soi.toString)

    val (fieldRefs, fieldOrdinals) = if (!mutableRowRecordId.isEmpty) { // Check if we need to include rowIds
      nonPartitionKeyAttrs.filterNot(x => isAttributeRowId(x._1)
        && soi.getStructFieldRef(x._1.name) == null).map {
        case (attr, ordinal) => soi.getStructFieldRef(attr.name) -> ordinal
      }.toArray.unzip
    } else {
      nonPartitionKeyAttrs.map { case (attr, ordinal) =>
        soi.getStructFieldRef(attr.name) -> ordinal
      }.toArray.unzip
    }

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
    // Find rowIdIndex
    val rowIdIndex = nonPartitionKeyAttrs.find(attrIdx => isAttributeRowId(attrIdx._1))
      .getOrElse((None, -1))._2
    if (rowIdIndex < 0 && !mutableRowRecordId.isEmpty) {
      HiveAcidErrors.unexpectedReadError("Error reading the ACID table" +
        " as rowId is not present even when includeRowId is the parameter")
    }
    iterator.map { value =>
      val raw = converter.convert(rawDeser.deserialize(value._2))
      var i = 0
      mutableRowRecordId match {
        case Some(record) =>
          val recordIdentifier = value._1
          record.setLong(0, recordIdentifier.getWriteId)
          record.setInt(1, recordIdentifier.getBucketProperty)
          record.setLong(2, recordIdentifier.getRowId)
          mutableRow.update(rowIdIndex, record)
        case None =>
      }
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

  def isAttributeRowId(attribute: Attribute): Boolean = {
    attribute.dataType.typeName == "struct" && attribute.name == "rowId"
  }
}
