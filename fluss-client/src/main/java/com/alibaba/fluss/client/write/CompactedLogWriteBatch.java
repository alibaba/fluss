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

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.record.MemoryLogRecordsCompactedBuilder;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.record.bytesview.MemorySegmentBytesView;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.rpc.messages.ProduceLogRequest;
import com.alibaba.fluss.utils.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * A batch of log records managed in COMPACTED format that is or will be sent to server by {@link
 * ProduceLogRequest}.
 *
 * <p>This class is not thread safe and external synchronization must be used when modifying it.
 */
@NotThreadSafe
@Internal
public class CompactedLogWriteBatch extends WriteBatch {
    private final MemoryLogRecordsCompactedBuilder recordsBuilder;

    public CompactedLogWriteBatch(
            TableBucket tableBucket,
            PhysicalTablePath physicalTablePath,
            MemoryLogRecordsCompactedBuilder recordsBuilder) {
        super(tableBucket, physicalTablePath);
        this.recordsBuilder = recordsBuilder;
    }

    @Override
    public boolean tryAppend(WriteRecord writeRecord, WriteCallback callback) throws Exception {
        InternalRow row = writeRecord.getRow();
        Preconditions.checkArgument(
                writeRecord.getTargetColumns() == null,
                "target columns must be null for log record");
        Preconditions.checkArgument(
                writeRecord.getKey() == null, "key must be null for log record");
        Preconditions.checkNotNull(row != null, "row must not be null for log record");
        Preconditions.checkNotNull(callback, "write callback must be not null");
        if (!recordsBuilder.hasRoomFor(row) || isClosed()) {
            return false;
        } else {
            recordsBuilder.append(RowKind.APPEND_ONLY, row);
            recordCount++;
            callbacks.add(callback);
            return true;
        }
    }

    @Override
    public void serialize() {
        // do nothing, records are serialized into memory buffer when appending
    }

    @Override
    public boolean trySerialize() {
        // records have been serialized.
        return true;
    }

    @VisibleForTesting
    public MemoryLogRecords records() {
        try {
            return recordsBuilder.build();
        } catch (IOException e) {
            throw new FlussRuntimeException("build memory log records failed", e);
        }
    }

    @Override
    public BytesView build() {
        MemoryLogRecords memoryLogRecords = records();
        return new MemorySegmentBytesView(
                memoryLogRecords.getMemorySegment(),
                memoryLogRecords.getPosition(),
                memoryLogRecords.sizeInBytes());
    }

    @Override
    public boolean isClosed() {
        return recordsBuilder.isClosed();
    }

    @Override
    public void close() throws Exception {
        recordsBuilder.close();
        reopened = false;
    }

    @Override
    public List<MemorySegment> memorySegments() {
        return Collections.singletonList(recordsBuilder.getMemorySegment());
    }

    @Override
    public void setWriterState(long writerId, int batchSequence) {
        recordsBuilder.setWriterState(writerId, batchSequence);
    }

    @Override
    public long writerId() {
        return recordsBuilder.writerId();
    }

    @Override
    public int batchSequence() {
        return recordsBuilder.batchSequence();
    }

    public void resetWriterState(long writerId, int batchSequence) {
        super.resetWriterState(writerId, batchSequence);
        recordsBuilder.resetWriterState(writerId, batchSequence);
    }

    @Override
    public int sizeInBytes() {
        return recordsBuilder.getSizeInBytes();
    }
}
