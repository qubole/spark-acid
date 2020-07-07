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

package com.qubole.spark.hiveacid.reader.v2;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import com.qubole.shaded.orc.OrcConf;
import com.qubole.shaded.orc.OrcFile;
import com.qubole.shaded.orc.Reader;
import com.qubole.shaded.orc.TypeDescription;
import com.qubole.shaded.orc.mapred.OrcInputFormat;
import com.qubole.shaded.hadoop.hive.common.type.HiveDecimal;
import com.qubole.shaded.hadoop.hive.ql.exec.vector.*;
import com.qubole.shaded.hadoop.hive.serde2.io.HiveDecimalWritable;
import com.qubole.shaded.hadoop.hive.ql.io.orc.OrcSplit;
import com.qubole.shaded.hadoop.hive.ql.io.orc.VectorizedOrcAcidRowBatchReader;
import com.qubole.shaded.hadoop.hive.ql.plan.MapWork;
import com.qubole.shaded.hadoop.hive.ql.exec.Utilities;
import com.qubole.shaded.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import com.qubole.shaded.orc.OrcProto;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.NullWritable;
import com.qubole.shaded.orc.OrcUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import com.qubole.shaded.hadoop.hive.ql.io.sarg.*;

/**
 * After creating, `initialize` and `initBatch` should be called sequentially. This internally uses
 * the Hive ACID Vectorized ORC reader to support reading of deleted and updated data.
 */
public class OrcColumnarBatchReader extends RecordReader<Void, ColumnarBatch> {

  // The capacity of vectorized batch.
  private int capacity;

  // Vectorized ORC Row Batch
  private VectorizedRowBatch batch;

  // ROW schema. This has the columns that needs to be projected. Even though we dont need
  // the ACID related columns like row id, write-id are also projected so that ACID reader can
  // use it to filter out the deleted/updated records.
  private TypeDescription schema;


  // The column IDs of the physical ORC file schema which are required by this reader.
  // -1 means this required column doesn't exist in the ORC file.
  private int[] requestedColIds;

  private StructField[] requiredFields;

  // Record reader from ORC row batch.
  private com.qubole.shaded.orc.RecordReader baseRecordReader;

  // Wrapper reader over baseRecordReader for filtering out deleted/updated records.
  private VectorizedOrcAcidRowBatchReader fullAcidRecordReader;

  // The result columnar batch for vectorized execution by whole-stage codegen.
  private ColumnarBatch columnarBatch;

  // Writable column vectors of the result columnar batch.
  private WritableColumnVector[] columnVectors;

  // The wrapped ORC column vectors.
  private org.apache.spark.sql.vectorized.ColumnVector[] orcVectorWrappers;

  // File(split) to be read.
  private OrcSplit fileSplit;

  private Configuration conf;

  // For full ACID scan, the first 5 fields are transaction related. These fields are used by
  // fullAcidRecordReader. While forming the batch to emit we skip the first 5 columns. For
  // normal scan, this value will be 0 as ORC file will not have the transaction related columns.
  private int rootColIdx;


  // Constructor.
  public OrcColumnarBatchReader(int capacity) {
    this.capacity = capacity;
  }

  @Override
  public Void getCurrentKey() {
    return null;
  }

  @Override
  public ColumnarBatch getCurrentValue() {
    return columnarBatch;
  }

  @Override
  public float getProgress() throws IOException {
    if (fullAcidRecordReader != null) {
      return fullAcidRecordReader.getProgress();
    } else {
      return baseRecordReader.getProgress();
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    return nextBatch();
  }

  @Override
  public void close() throws IOException {
    if (columnarBatch != null) {
      columnarBatch.close();
      columnarBatch = null;
    }
    if (fullAcidRecordReader != null) {
      fullAcidRecordReader.close();
      fullAcidRecordReader = null;
    }
    if (baseRecordReader != null) {
      baseRecordReader.close();
      baseRecordReader = null;
    }
  }

  // The columns that are pushed as search arguments to ORC file reader.
  private String[] getSargColumnNames(String[] originalColumnNames,
                                      List<OrcProto.Type> types,
                                      boolean[] includedColumns) {
    // Skip ACID related columns if present.
    String[] columnNames = new String[types.size() - rootColIdx];
    int i = 0;
    Iterator iterator = ((OrcProto.Type)types.get(rootColIdx)).getSubtypesList().iterator();

    while(true) {
      int columnId;
      do {
        if (!iterator.hasNext()) {
          return columnNames;
        }
        columnId = (Integer)iterator.next();
      } while(includedColumns != null && !includedColumns[columnId - rootColIdx]);
      columnNames[columnId - rootColIdx] = originalColumnNames[i++];
    }
  }

  private void setSearchArgument(Reader.Options options,
                                 List<OrcProto.Type> types,
                                 Configuration conf) {
    String neededColumnNames = conf.get("hive.io.file.readcolumn.names");
    if (neededColumnNames == null) {
      options.searchArgument((SearchArgument)null, (String[])null);
    } else {
      // The filters which are pushed down are set in config using sarg.pushdown.
      SearchArgument sarg = ConvertAstToSearchArg.createFromConf(conf);
      if (sarg == null) {
        options.searchArgument((SearchArgument)null, (String[])null);
      } else {
        String[] colNames = getSargColumnNames(neededColumnNames.split(","),
                types, options.getInclude());
        options.searchArgument(sarg, colNames);
      }
    }
  }

  private void setSearchArgumentForOption(Configuration conf,
                                          TypeDescription readerSchema,
                                          Reader.Options readerOptions) {
    final List<OrcProto.Type> schemaTypes = OrcUtils.getOrcTypes(readerSchema);
    setSearchArgument(readerOptions, schemaTypes, conf);
  }

  /**
   * Initialize ORC file reader and batch record reader.
   * Please note that `initBatch` is needed to be called after this.
   */
  @Override
  public void initialize(InputSplit inputSplit,
                         TaskAttemptContext taskAttemptContext) {
    fileSplit = (OrcSplit)inputSplit;
    conf = taskAttemptContext.getConfiguration();
  }

  // Wrapper ACID reader over base ORC record reader.
  private VectorizedOrcAcidRowBatchReader initHiveAcidReader(Configuration conf,
                                                             OrcSplit orcSplit,
                                                             com.qubole.shaded.orc.RecordReader innerReader) {
    conf.set("hive.vectorized.execution.enabled", "true");
    MapWork mapWork = new MapWork();
    VectorizedRowBatchCtx rbCtx = new VectorizedRowBatchCtx();
    mapWork.setVectorMode(true);
    mapWork.setVectorizedRowBatchCtx(rbCtx);
    Utilities.setMapWork(conf, mapWork);

    org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch> baseReader
            = new org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch>() {

      @Override
      public boolean next(NullWritable key, VectorizedRowBatch value) throws IOException {
        // This is the baseRecordReader which will be called internally by ACID reader to fetch
        // records.
        return innerReader.nextBatch(value);
      }

      @Override
      public NullWritable createKey() {
        return NullWritable.get();
      }

      @Override
      public VectorizedRowBatch createValue() {
        // Tis column batch will be passed as value by ACID reader while calling next. So the
        // baseRecordReader will populate the batch directly and we dont have to do any
        // extra copy if selected in use is false.
        return batch;
      }

      @Override
      public long getPos() throws IOException {
        return 0;
      }

      @Override
      public void close() throws IOException {
        innerReader.close();
      }

      @Override
      public float getProgress() throws IOException {
        return innerReader.getProgress();
      }
    };

    try {
      return new VectorizedOrcAcidRowBatchReader(orcSplit, new JobConf(conf), Reporter.NULL, baseReader, rbCtx, true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Initialize columnar batch by setting required schema and partition information.
   * With this information, this creates ColumnarBatch with the full schema.
   */
  public void initBatch(
          TypeDescription orcSchema,
          int[] requestedColIds,
          StructField[] requiredFields,
          StructType partitionSchema,
          InternalRow partitionValues,
          boolean isAcidScan) throws IOException {

    if (!isAcidScan) {
      //rootCol = org.apache.hadoop.hive.ql.io.orc.OrcInputFormat.getRootColumn(true);
      rootColIdx = 0;
    } else {
      //rootCol = org.apache.hadoop.hive.ql.io.orc.OrcInputFormat.getRootColumn(false) - 1;
      // In ORC, for full ACID scan, the first 5 fields stores the transaction metadata.
      rootColIdx = 5;
    }

    // Create the baseRecordReader. This reader actually does the reading from ORC file.
    Reader readerInner = OrcFile.createReader(
            fileSplit.getPath(), OrcFile.readerOptions(conf)
                    .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(conf))
                    .filesystem(fileSplit.getPath().getFileSystem(conf)));
    Reader.Options options = /*createOptionsForReader(conf, orcSchema);*/
            OrcInputFormat.buildOptions(conf, readerInner, fileSplit.getStart(), fileSplit.getLength());
    setSearchArgumentForOption(conf, orcSchema, options);
    baseRecordReader = readerInner.rows(options);

    // This schema will have both required fields and the filed to be used by ACID reader.
    schema = orcSchema;
    batch = orcSchema.createRowBatch(capacity);
    assert(!batch.selectedInUse); // `selectedInUse` should be initialized with `false`.

    this.requiredFields = requiredFields;
    this.requestedColIds = requestedColIds;
    assert(requiredFields.length == requestedColIds.length);

    // The result schema will just have those fields which are required to be projected.
    StructType resultSchema = new StructType(requiredFields);
    for (StructField f : partitionSchema.fields()) {
      resultSchema = resultSchema.add(f);
    }

    // For ACID scan, the ACID batch reader might filter out some of the records read from
    // ORC file. So we have to recreate the batch read from ORC files. This columnVectors
    // will be used during that time. Missing columns and partition columns are filled here
    // and other valid columns will be filled once the batch of record is read.
    //TODO:We can set the config to let ORC reader fill the partition values.
    if (isAcidScan) {
      columnVectors = OnHeapColumnVector.allocateColumns(capacity, resultSchema);

      // Initialize the missing columns once.
      for (int i = 0; i < requiredFields.length; i++) {
        if (requestedColIds[i] == -1) {
          columnVectors[i].putNulls(0, capacity);
          columnVectors[i].setIsConstant();
        }
      }

      if (partitionValues.numFields() > 0) {
        int partitionIdx = requiredFields.length;
        for (int i = 0; i < partitionValues.numFields(); i++) {
          ColumnVectorUtils.populate(columnVectors[i + partitionIdx], partitionValues, i);
          columnVectors[i + partitionIdx].setIsConstant();
        }
      }
    }


    // Just wrap the ORC column vector instead of copying it to Spark column vector. This wrapper
    // will be used for insert only table scan or scanning original files (ACID V1) or compacted
    // file. In those cases, the batch read from ORC will be emitted as it is. So no need to
    // prepare a separate copy.

    ColumnVector[] fields;
    orcVectorWrappers = new org.apache.spark.sql.vectorized.ColumnVector[resultSchema.length()];
    if (rootColIdx == 0) {
      fields  = batch.cols;
    } else {
      fields  = ((StructColumnVector)batch.cols[rootColIdx]).fields;
    }

    orcVectorWrappers = new org.apache.spark.sql.vectorized.ColumnVector[resultSchema.length()];
    //StructColumnVector dataCols  = (StructColumnVector)batch.cols[5];
    for (int i = 0; i < requiredFields.length; i++) {
      DataType dt = requiredFields[i].dataType();
      int colId = requestedColIds[i];
      // Initialize the missing columns once.
      if (colId == -1) {
        OnHeapColumnVector missingCol = new OnHeapColumnVector(capacity, dt);
        missingCol.putNulls(0, capacity);
        missingCol.setIsConstant();
        orcVectorWrappers[i] = missingCol;
      } else {
        orcVectorWrappers[i] = new HiveAcidColumnVector(dt, fields[colId]);
      }
    }

    if (partitionValues.numFields() > 0) {
      int partitionIdx = requiredFields.length;
      for (int i = 0; i < partitionValues.numFields(); i++) {
        DataType dt = partitionSchema.fields()[i].dataType();
        OnHeapColumnVector partitionCol = new OnHeapColumnVector(capacity, dt);
        ColumnVectorUtils.populate(partitionCol, partitionValues, i);
        partitionCol.setIsConstant();
        orcVectorWrappers[partitionIdx + i] = partitionCol;
      }
    }

    if (isAcidScan) {
      fullAcidRecordReader = initHiveAcidReader(conf, fileSplit, baseRecordReader);
    } else {
      fullAcidRecordReader = null;
    }
  }

  /**
   * Return true if there exists more data in the next batch. For acid scan, the ACID batch
   * reader is used. The ACID batch reader internally uses the baseRecordReader and then
   * filters out the deleted/not visible records. This filter is propagated here using
   * selectedInUse. If selectedInUse is false, that means there is no filtering happened
   * so we can directly use the orcVectorWrappers. If selectedInUse is set to true, we
   * have to recreate the column batch using selected array.
   */
  private boolean nextBatch() throws IOException {
    VectorizedRowBatch vrb;
    if (fullAcidRecordReader != null) {
      vrb = schema.createRowBatch(capacity);
      // Internally Acid batch reader changes the batch schema. So vrb is passed instead of batch.
      if (!fullAcidRecordReader.next(NullWritable.get(), vrb)) {
        // Should not use batch size for fullAcidRecordReader. The batch size may be 0 in some cases
        // where whole batch of records are filtered out.
        return false;
      }
    } else {
      if (!baseRecordReader.nextBatch(batch)) {
        //TODO: Should we return false if batch size is 0?
        return false;
      }
      vrb = batch;
    }

    int batchSize = vrb.size;

    // selectedInUse is false means no filtering is done. We can use the wrapper directly. No need to
    // recreate the column batch.
    if (!vrb.selectedInUse) {
      for (int i = 0; i < requiredFields.length; i++) {
        if (requestedColIds[i] != -1) {
          ((HiveAcidColumnVector) orcVectorWrappers[i]).setBatchSize(batchSize);
        }
      }
      columnarBatch = new ColumnarBatch(orcVectorWrappers);
      columnarBatch.setNumRows(batchSize);
      return true;
    }


    // Recreate the batch using selected array. For those records with selected[idx] == 0, remove
    // those from the resultant batch. So its possible that the batch size will become 0, but still we
    // should return true, so that the caller calls next again. Before that we should reset the column
    // vector to inform user that no data is there.
    for (WritableColumnVector toColumn : columnVectors) {
      toColumn.reset();
    }

    if (batchSize > 0) {
      StructColumnVector dataCols  = (StructColumnVector)vrb.cols[rootColIdx];
      for (int i = 0; i < requiredFields.length; i++) {
        StructField field = requiredFields[i];
        WritableColumnVector toColumn = columnVectors[i];
        if (requestedColIds[i] >= 0) {
          ColumnVector fromColumn = dataCols.fields[requestedColIds[i]];
          if (fromColumn.isRepeating) {
            putRepeatingValues(batchSize, field, fromColumn, toColumn);
          } else if (fromColumn.noNulls) {
            putNonNullValues(batchSize, field, fromColumn, toColumn, vrb.selected);
          } else {
            putValues(batchSize, field, fromColumn, toColumn, vrb.selected);
          }
        }
      }
    }

    columnarBatch = new ColumnarBatch(columnVectors);
    columnarBatch.setNumRows(batchSize);
    return true;
  }

  private void putRepeatingValues(
          int batchSize,
          StructField field,
          ColumnVector fromColumn,
          WritableColumnVector toColumn) {
    if (fromColumn.isNull[0]) {
      toColumn.putNulls(0, batchSize);
    } else {
      DataType type = field.dataType();
      if (type instanceof BooleanType) {
        toColumn.putBooleans(0, batchSize, ((LongColumnVector)fromColumn).vector[0] == 1);
      } else if (type instanceof ByteType) {
        toColumn.putBytes(0, batchSize, (byte)((LongColumnVector)fromColumn).vector[0]);
      } else if (type instanceof ShortType) {
        toColumn.putShorts(0, batchSize, (short)((LongColumnVector)fromColumn).vector[0]);
      } else if (type instanceof IntegerType || type instanceof DateType) {
        toColumn.putInts(0, batchSize, (int)((LongColumnVector)fromColumn).vector[0]);
      } else if (type instanceof LongType) {
        toColumn.putLongs(0, batchSize, ((LongColumnVector)fromColumn).vector[0]);
      } else if (type instanceof TimestampType) {
        toColumn.putLongs(0, batchSize,
                fromTimestampColumnVector((TimestampColumnVector)fromColumn, 0));
      } else if (type instanceof FloatType) {
        toColumn.putFloats(0, batchSize, (float)((DoubleColumnVector)fromColumn).vector[0]);
      } else if (type instanceof DoubleType) {
        toColumn.putDoubles(0, batchSize, ((DoubleColumnVector)fromColumn).vector[0]);
      } else if (type instanceof StringType || type instanceof BinaryType) {
        BytesColumnVector data = (BytesColumnVector)fromColumn;
        int size = data.vector[0].length;
        toColumn.arrayData().reserve(size);
        toColumn.arrayData().putBytes(0, size, data.vector[0], 0);
        for (int index = 0; index < batchSize; index++) {
          toColumn.putArray(index, 0, size);
        }
      } else if (type instanceof DecimalType) {
        DecimalType decimalType = (DecimalType)type;
        putDecimalWritables(
                toColumn,
                batchSize,
                decimalType.precision(),
                decimalType.scale(),
                ((DecimalColumnVector)fromColumn).vector[0]);
      } else {
        throw new UnsupportedOperationException("Unsupported Data Type: " + type);
      }
    }
  }

  private void putNonNullValues(
          int batchSize,
          StructField field,
          ColumnVector fromColumn,
          WritableColumnVector toColumn,
          int[] selected) {
    DataType type = field.dataType();
    if (type instanceof BooleanType) {
      long[] data = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        toColumn.putBoolean(index, data[logicalIdx] == 1);
      }
    } else if (type instanceof ByteType) {
      long[] data = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        toColumn.putByte(index, (byte)data[logicalIdx]);
      }
    } else if (type instanceof ShortType) {
      long[] data = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        toColumn.putShort(index, (short)data[logicalIdx]);
      }
    } else if (type instanceof IntegerType || type instanceof DateType) {
      long[] data = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        toColumn.putInt(index, (int)data[logicalIdx]);
      }
    } else if (type instanceof LongType) {
      long[] data = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        toColumn.putLong(index, data[logicalIdx]);
      }
      //toColumn.putLongs(0, batchSize, ((LongColumnVector)fromColumn).vector, 0);
    } else if (type instanceof TimestampType) {
      TimestampColumnVector data = ((TimestampColumnVector)fromColumn);
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        toColumn.putLong(index, fromTimestampColumnVector(data, logicalIdx));
      }
    } else if (type instanceof FloatType) {
      double[] data = ((DoubleColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        toColumn.putFloat(index, (float)data[logicalIdx]);
      }
    } else if (type instanceof DoubleType) {
      //toColumn.putDoubles(0, batchSize, ((DoubleColumnVector)fromColumn).vector, 0);
      double[] data = ((DoubleColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        toColumn.putDouble(index, data[logicalIdx]);
      }
    } else if (type instanceof StringType || type instanceof BinaryType) {
      BytesColumnVector data = ((BytesColumnVector)fromColumn);
      WritableColumnVector arrayData = toColumn.arrayData();
      int totalNumBytes = IntStream.of(data.length).sum();
      arrayData.reserve(totalNumBytes);
      for (int index = 0, pos = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        arrayData.putBytes(pos, data.length[logicalIdx], data.vector[logicalIdx], data.start[logicalIdx]);
        toColumn.putArray(index, pos, data.length[logicalIdx]);
        pos += data.length[logicalIdx];
      }
    } else if (type instanceof DecimalType) {
      DecimalType decimalType = (DecimalType)type;
      DecimalColumnVector data = ((DecimalColumnVector)fromColumn);
      if (decimalType.precision() > Decimal.MAX_LONG_DIGITS()) {
        toColumn.arrayData().reserve(batchSize * 16);
      }
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        putDecimalWritable(
                toColumn,
                index,
                decimalType.precision(),
                decimalType.scale(),
                data.vector[logicalIdx]);
      }
    } else {
      throw new UnsupportedOperationException("Unsupported Data Type: " + type);
    }
  }

  private void putValues(
          int batchSize,
          StructField field,
          ColumnVector fromColumn,
          WritableColumnVector toColumn,
          int[] selected) {
    DataType type = field.dataType();
    if (type instanceof BooleanType) {
      long[] vector = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          toColumn.putBoolean(index, vector[logicalIdx] == 1);
        }
      }
    } else if (type instanceof ByteType) {
      long[] vector = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          toColumn.putByte(index, (byte)vector[logicalIdx]);
        }
      }
    } else if (type instanceof ShortType) {
      long[] vector = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          toColumn.putShort(index, (short)vector[logicalIdx]);
        }
      }
    } else if (type instanceof IntegerType || type instanceof DateType) {
      long[] vector = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          toColumn.putInt(index, (int)vector[logicalIdx]);
        }
      }
    } else if (type instanceof LongType) {
      long[] vector = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          toColumn.putLong(index, vector[logicalIdx]);
        }
      }
    } else if (type instanceof TimestampType) {
      TimestampColumnVector vector = ((TimestampColumnVector)fromColumn);
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          toColumn.putLong(index, fromTimestampColumnVector(vector, logicalIdx));
        }
      }
    } else if (type instanceof FloatType) {
      double[] vector = ((DoubleColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          toColumn.putFloat(index, (float)vector[logicalIdx]);
        }
      }
    } else if (type instanceof DoubleType) {
      double[] vector = ((DoubleColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          toColumn.putDouble(index, vector[logicalIdx]);
        }
      }
    } else if (type instanceof StringType || type instanceof BinaryType) {
      BytesColumnVector vector = (BytesColumnVector)fromColumn;
      WritableColumnVector arrayData = toColumn.arrayData();
      int totalNumBytes = IntStream.of(vector.length).sum();
      arrayData.reserve(totalNumBytes);
      for (int index = 0, pos = 0; index < batchSize; pos += vector.length[index], index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          arrayData.putBytes(pos, vector.length[logicalIdx], vector.vector[logicalIdx], vector.start[logicalIdx]);
          toColumn.putArray(index, pos, vector.length[logicalIdx]);
        }
      }
    } else if (type instanceof DecimalType) {
      DecimalType decimalType = (DecimalType)type;
      HiveDecimalWritable[] vector = ((DecimalColumnVector)fromColumn).vector;
      if (decimalType.precision() > Decimal.MAX_LONG_DIGITS()) {
        toColumn.arrayData().reserve(batchSize * 16);
      }
      for (int index = 0; index < batchSize; index++) {
        int logicalIdx = selected[index];
        if (fromColumn.isNull[logicalIdx]) {
          toColumn.putNull(index);
        } else {
          putDecimalWritable(
                  toColumn,
                  index,
                  decimalType.precision(),
                  decimalType.scale(),
                  vector[logicalIdx]);
        }
      }
    } else {
      throw new UnsupportedOperationException("Unsupported Data Type: " + type);
    }
  }

  /**
   * Returns the number of micros since epoch from an element of TimestampColumnVector.
   */
  private static long fromTimestampColumnVector(TimestampColumnVector vector, int index) {
    return vector.time[index] * 1000 + (vector.nanos[index] / 1000 % 1000);
  }

  /**
   * Put a `HiveDecimalWritable` to a `WritableColumnVector`.
   */
  private static void putDecimalWritable(
          WritableColumnVector toColumn,
          int index,
          int precision,
          int scale,
          HiveDecimalWritable decimalWritable) {
    HiveDecimal decimal = decimalWritable.getHiveDecimal();
    Decimal value =
            Decimal.apply(decimal.bigDecimalValue(), decimal.precision(), decimal.scale());
    value.changePrecision(precision, scale);

    if (precision <= Decimal.MAX_INT_DIGITS()) {
      toColumn.putInt(index, (int) value.toUnscaledLong());
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      toColumn.putLong(index, value.toUnscaledLong());
    } else {
      byte[] bytes = value.toJavaBigDecimal().unscaledValue().toByteArray();
      toColumn.arrayData().putBytes(index * 16, bytes.length, bytes, 0);
      toColumn.putArray(index, index * 16, bytes.length);
    }
  }

  /**
   * Put `HiveDecimalWritable`s to a `WritableColumnVector`.
   */
  private static void putDecimalWritables(
          WritableColumnVector toColumn,
          int size,
          int precision,
          int scale,
          HiveDecimalWritable decimalWritable) {
    HiveDecimal decimal = decimalWritable.getHiveDecimal();
    Decimal value =
            Decimal.apply(decimal.bigDecimalValue(), decimal.precision(), decimal.scale());
    value.changePrecision(precision, scale);

    if (precision <= Decimal.MAX_INT_DIGITS()) {
      toColumn.putInts(0, size, (int) value.toUnscaledLong());
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      toColumn.putLongs(0, size, value.toUnscaledLong());
    } else {
      byte[] bytes = value.toJavaBigDecimal().unscaledValue().toByteArray();
      toColumn.arrayData().reserve(bytes.length);
      toColumn.arrayData().putBytes(0, bytes.length, bytes, 0);
      for (int index = 0; index < size; index++) {
        toColumn.putArray(index, 0, bytes.length);
      }
    }
  }
}
