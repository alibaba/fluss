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

package com.alibaba.fluss.record;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.MemorySegmentOutputView;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.MemoryAwareGetters;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.row.indexed.IndexedRowWriter;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.utils.MurmurHashUtils;

import java.io.IOException;

import static com.alibaba.fluss.record.MemoryLogRecordBatch.LENGTH_LENGTH;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an immutable log record and can be directly persisted. The schema is as follows:
 *
 * <ul>
 *   <li>Length => int32
 *   <li>Attributes => Int8
 *   <li>Value => {@link InternalRow}
 * </ul>
 *
 * <p>The current record attributes are depicted below:
 *
 * <p>----------- | RowKind (0-3) | Unused (4-7) |---------------
 *
 * <p>The offset compute the difference relative to the base offset and of the batch that this
 * record is contained in.
 *
 * @since 0.1
 */
@PublicEvolving
// TODO: should rename to IndexedLogRecord as it only indexed row?
public class DefaultLogRecord implements LogRecord {

    private static final int ATTRIBUTES_LENGTH = 1;

    private final long logOffset;
    private final long timestamp;
    private final DataType[] fieldTypes;

    private MemorySegment segment;
    private int offset;
    private int sizeInBytes;

    DefaultLogRecord(long logOffset, long timestamp, DataType[] fieldTypes) {
        this.logOffset = logOffset;
        this.fieldTypes = fieldTypes;
        this.timestamp = timestamp;
    }

    private void pointTo(MemorySegment segment, int offset, int sizeInBytes) {
        this.segment = segment;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DefaultLogRecord that = (DefaultLogRecord) o;
        return sizeInBytes == that.sizeInBytes
                && segment.equalTo(that.segment, offset, that.offset, sizeInBytes);
    }

    @Override
    public int hashCode() {
        return MurmurHashUtils.hashBytes(segment, offset, sizeInBytes);
    }

    @Override
    public long logOffset() {
        return logOffset;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public RowKind getRowKind() {
        byte attributes = segment.get(offset + LENGTH_LENGTH);
        return RowKind.fromByteValue(attributes);
    }

    @Override
    public InternalRow getRow() {
        int rowOffset = LENGTH_LENGTH + ATTRIBUTES_LENGTH;
        // TODO currently, we only support indexed row.
        return deserializeInternalRow(
                sizeInBytes - rowOffset,
                segment,
                offset + rowOffset,
                fieldTypes,
                LogFormat.INDEXED);
    }

    /** Write the record to input `target` and return its size. */
    public static int writeTo(MemorySegmentOutputView outputView, RowKind rowKind, InternalRow row)
            throws IOException {
        int sizeInBytes = calculateSizeInBytes(row);

        // TODO using varint instead int to reduce storage size.
        // write record total bytes size.
        outputView.writeInt(sizeInBytes);

        // write attributes.
        outputView.writeByte(rowKind.toByteValue());

        // write internal row.
        serializeInternalRow(outputView, row);

        return sizeInBytes + LENGTH_LENGTH;
    }

    public static DefaultLogRecord readFrom(
            MemorySegment segment,
            int position,
            long logOffset,
            long logTimestamp,
            DataType[] colTypes) {
        int sizeInBytes = segment.getInt(position);
        DefaultLogRecord logRecord = new DefaultLogRecord(logOffset, logTimestamp, colTypes);
        logRecord.pointTo(segment, position, sizeInBytes + LENGTH_LENGTH);
        return logRecord;
    }

    public static int sizeOf(InternalRow row) {
        int sizeInBytes = calculateSizeInBytes(row);
        return sizeInBytes + LENGTH_LENGTH;
    }

    private static int calculateSizeInBytes(InternalRow row) {
        int size = 1; // always one byte for attributes
        size += ((MemoryAwareGetters) row).getSizeInBytes();
        return size;
    }

    private static void serializeInternalRow(
            MemorySegmentOutputView outputView, InternalRow internalRow) throws IOException {
        if (internalRow instanceof IndexedRow) {
            IndexedRow indexedRow = (IndexedRow) internalRow;
            IndexedRowWriter.serializeIndexedRow(indexedRow, outputView);
        } else {
            throw new IllegalArgumentException(
                    "No such internal row serializer for: "
                            + internalRow.getClass().getSimpleName());
        }
    }

    private static InternalRow deserializeInternalRow(
            int length,
            MemorySegment segment,
            int position,
            DataType[] fieldTypes,
            LogFormat logFormat) {
        if (logFormat == LogFormat.INDEXED) {
            IndexedRow indexedRow = new IndexedRow(fieldTypes);
            indexedRow.pointTo(segment, position, length);
            return indexedRow;
        } else {
            throw new IllegalArgumentException(
                    "No such internal row deserializer for: " + logFormat);
        }
    }
}
