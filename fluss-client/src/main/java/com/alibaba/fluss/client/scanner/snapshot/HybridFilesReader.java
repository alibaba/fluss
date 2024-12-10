/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.scanner.snapshot;

import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.client.scanner.log.LogScanner;
import com.alibaba.fluss.client.scanner.log.ScanRecords;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.row.compacted.CompactedRow;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.row.encode.RowEncoder;
import com.alibaba.fluss.row.encode.ValueEncoder;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.utils.IOUtils;

import org.rocksdb.RocksDBException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * A reader to read kv snapshot files to {@link ScanRecord}s and merge a piece of log. It will
 * return the {@link ScanRecord}s as an iterator.
 */
@NotThreadSafe
public class HybridFilesReader extends SnapshotFilesReader {
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(10000L);

    private final short schemaId;
    private final KeyEncoder keyEncoder;
    private final RowEncoder rowEncoder;
    private final KvFormat kvFormat;
    private final DataType[] fieldTypes;
    private final long startingOffset;
    private final long stoppingOffset;
    private final TableBucket tableBucket;
    private final LogScanner logScanner;

    // key is index of a projected field in the whole table, value is index of the projected field
    // in whole project fields
    private final Map<Integer, Integer> logProjectedFieldSet;

    InternalRow.FieldGetter[] logFieldGetters;

    public HybridFilesReader(
            KvFormat kvFormat,
            Path rocksDbPath,
            short schemaId,
            Schema tableSchema,
            @Nullable int[] targetProjectedFields,
            @Nullable int[] logProjectedFields,
            long startingOffset,
            long stoppingOffset,
            TableBucket tableBucket,
            LogScanner logScanner) {
        super(kvFormat, rocksDbPath, tableSchema, targetProjectedFields);
        this.schemaId = schemaId;
        this.keyEncoder =
                new KeyEncoder(tableSchema.toRowType(), tableSchema.getPrimaryKeyIndexes());
        DataType[] dataTypes = tableSchema.toRowType().getChildren().toArray(new DataType[0]);
        this.rowEncoder = RowEncoder.create(kvFormat, dataTypes);
        this.kvFormat = kvFormat;
        this.fieldTypes = dataTypes;
        this.startingOffset = startingOffset;
        this.stoppingOffset = stoppingOffset;
        this.tableBucket = tableBucket;
        this.logScanner = logScanner;

        if (logProjectedFields == null) {
            logProjectedFields = IntStream.range(0, dataTypes.length).toArray();
        }

        this.logProjectedFieldSet = new HashMap<>();
        for (int i = 0; i < logProjectedFields.length; i++) {
            logProjectedFieldSet.put(logProjectedFields[i], i);
        }

        logFieldGetters = new InternalRow.FieldGetter[logProjectedFields.length];
        for (int i = 0; i < logProjectedFields.length; i++) {
            DataType fieldDataType = dataTypes[logProjectedFields[i]];
            logFieldGetters[i] = InternalRow.createFieldGetter(fieldDataType, i);
        }
    }

    /** Override init to subscribe and apply logs before init iterator. */
    @Override
    public void init() throws IOException {
        try {
            initRocksDB(rocksDbPath, false);
            subscribeAndApplyLogs();
            initRocksIterator();
        } catch (Throwable t) {
            releaseSnapshot();
            // If anything goes wrong, clean up our stuff. If things went smoothly the
            // merging iterator is now responsible for closing the resources
            IOUtils.closeQuietly(closeableRegistry);
            throw new IOException("Error creating RocksDB snapshot reader.", t);
        }
    }

    private void subscribeAndApplyLogs() throws RocksDBException {
        if (startingOffset >= stoppingOffset || stoppingOffset == 0) {
            return;
        }

        if (tableBucket.getPartitionId() != null) {
            logScanner.subscribe(
                    tableBucket.getPartitionId(), tableBucket.getBucket(), startingOffset);
        } else {
            logScanner.subscribe(tableBucket.getBucket(), startingOffset);
        }

        boolean readEnd = false;
        do {
            ScanRecords scanRecords = logScanner.poll(POLL_TIMEOUT);
            for (ScanRecord scanRecord : scanRecords) {
                // apply log to snapshot.
                if (scanRecord.getOffset() <= stoppingOffset - 1) {
                    applyLogs(scanRecord);
                }
                if (scanRecord.getOffset() >= stoppingOffset - 1) {
                    readEnd = true;
                    break;
                }
            }

        } while (!readEnd);
    }

    private void applyLogs(ScanRecord scanRecord) throws RocksDBException {
        BinaryRow row = castProjectRowToEntireRow(scanRecord.getRow());
        byte[] key = keyEncoder.encode(row);
        switch (scanRecord.getRowKind()) {
            case APPEND_ONLY:
                throw new UnsupportedOperationException(
                        "Hybrid File Reader can not apply append only logs.");
            case INSERT:
            case UPDATE_AFTER:
                byte[] value = ValueEncoder.encodeValue(schemaId, row);
                db.put(key, value);
                break;
            case DELETE:
            case UPDATE_BEFORE:
                db.delete(key);
        }
    }

    /**
     * The row of log is projection result while the row of snapshot is entire row, thus need to put
     * a placeholder value at un-projection indexes.
     */
    private BinaryRow castProjectRowToEntireRow(InternalRow row) {
        if (KvFormat.COMPACTED.equals(kvFormat)) {
            return castToAnEntireCompactedRow(row);
        } else {
            return castToAnEntireIndexedRow(row);
        }
    }

    private BinaryRow castToAnEntireIndexedRow(InternalRow row) {
        rowEncoder.startNewRow();
        for (Integer projectField : logProjectedFieldSet.keySet()) {
            rowEncoder.encodeField(
                    projectField,
                    logFieldGetters[logProjectedFieldSet.get(projectField)].getFieldOrNull(row));
        }
        return rowEncoder.finishRow();
    }

    private BinaryRow castToAnEntireCompactedRow(InternalRow row) {
        if (row instanceof CompactedRow) {
            return (CompactedRow) row;
        }
        rowEncoder.startNewRow();
        for (int i = 0; i < fieldTypes.length; i++) {
            if (logProjectedFieldSet.containsKey(i)) {
                rowEncoder.encodeField(
                        i, logFieldGetters[logProjectedFieldSet.get(i)].getFieldOrNull(row));
            } else {
                // When use ProjectedRow to read projection columns from compacted row in rocksdb,
                // deserialize the entire row at first. Thus, must put into placeholder value though
                // it is no use later, nor un-projection columns maybe out of bound.
                rowEncoder.encodeField(i, getPlaceHolderValueOfCompactedRow(fieldTypes[i]));
            }
        }
        return rowEncoder.finishRow();
    }

    private static Object getPlaceHolderValueOfCompactedRow(DataType fieldType) {
        if (fieldType.isNullable()) {
            return null;
        }

        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case STRING:
                return BinaryString.blankString(1);
            case BOOLEAN:
                return false;
            case BINARY:
            case BYTES:
                return new byte[0];
            case DECIMAL:
                return BigDecimal.ZERO;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return 0;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampNtz.now();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampLtz.fromInstant(Instant.MIN);
            default:
                throw new IllegalArgumentException(
                        "Unsupported type for CompactedRow: " + fieldType);
        }
    }
}
