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

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.MemorySegmentOutputView;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.CompactedLogRecord;
import com.alibaba.fluss.record.DefaultLogRecordBatch;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.record.LogRecordReadContext;
import com.alibaba.fluss.record.MemoryLogRecordsCompactedBuilder;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.record.bytesview.MemorySegmentBytesView;
import com.alibaba.fluss.row.compacted.CompactedRow;
import com.alibaba.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.alibaba.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO;
import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CompactedLogWriteBatch}. */
public class CompactedLogWriteBatchTest {
    private CompactedRow row;
    private int estimatedSizeInBytes;

    @BeforeEach
    void setup() {
        row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        estimatedSizeInBytes = CompactedLogRecord.sizeOf(row);
    }

    @Test
    void testTryAppendWithWriteLimit() throws Exception {
        int bucketId = 0;
        int writeLimit = 100;
        CompactedLogWriteBatch logProducerBatch =
                createLogWriteBatch(
                        new TableBucket(DATA1_TABLE_ID, bucketId),
                        0L,
                        writeLimit,
                        MemorySegment.allocateHeapMemory(writeLimit));
        for (int i = 0;
                i
                        < (writeLimit - DefaultLogRecordBatch.RECORD_BATCH_HEADER_SIZE)
                                / estimatedSizeInBytes;
                i++) {
            boolean appendResult =
                    logProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
            assertThat(appendResult).isTrue();
        }

        // batch full.
        boolean appendResult = logProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isFalse();
    }

    @Test
    void testToBytes() throws Exception {
        int bucketId = 0;
        CompactedLogWriteBatch logProducerBatch =
                createLogWriteBatch(new TableBucket(DATA1_TABLE_ID, bucketId), 0L);
        boolean appendResult = logProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isTrue();

        logProducerBatch.close();
        logProducerBatch.serialize();
        BytesView bytesView = logProducerBatch.build();
        DefaultLogRecordBatch recordBatch = new DefaultLogRecordBatch();
        MemorySegmentBytesView firstBytesView = (MemorySegmentBytesView) bytesView;
        recordBatch.pointTo(firstBytesView.getMemorySegment(), firstBytesView.getPosition());
        assertDefaultLogRecordBatchEquals(recordBatch);
    }

    @Test
    void testFailedTwice() throws Exception {
        int bucketId = 0;
        CompactedLogWriteBatch logWriteBatch =
                createLogWriteBatch(new TableBucket(DATA1_TABLE_ID, bucketId), 0L);
        boolean appendResult = logWriteBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isTrue();

        assertThat(logWriteBatch.completeExceptionally(new IllegalStateException("test failed.")))
                .isTrue();
        // FAILED --> FAILED transitions are ignored.
        assertThat(logWriteBatch.completeExceptionally(new IllegalStateException("test failed.")))
                .isFalse();
    }

    @Test
    void testClose() throws Exception {
        int bucketId = 0;
        CompactedLogWriteBatch logProducerBatch =
                createLogWriteBatch(new TableBucket(DATA1_TABLE_ID, bucketId), 0L);
        boolean appendResult = logProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isTrue();

        logProducerBatch.close();
        assertThat(logProducerBatch.isClosed()).isTrue();

        appendResult = logProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isFalse();
    }

    @Test
    void testCompleteTwice() throws Exception {
        int bucketId = 0;
        CompactedLogWriteBatch logWriteBatch =
                createLogWriteBatch(new TableBucket(DATA1_TABLE_ID, bucketId), 0L);
        boolean appendResult = logWriteBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isTrue();

        assertThat(logWriteBatch.complete()).isTrue();
        assertThatThrownBy(logWriteBatch::complete)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "A SUCCEEDED batch must not attempt another state change to SUCCEEDED");
    }

    private WriteRecord createWriteRecord() {
        return new WriteRecord(DATA1_PHYSICAL_TABLE_PATH, WriteKind.APPEND, row, null);
    }

    private CompactedLogWriteBatch createLogWriteBatch(TableBucket tb, long baseLogOffset)
            throws Exception {
        return createLogWriteBatch(
                tb, baseLogOffset, Integer.MAX_VALUE, MemorySegment.allocateHeapMemory(1000));
    }

    private CompactedLogWriteBatch createLogWriteBatch(
            TableBucket tb, long baseLogOffset, int writeLimit, MemorySegment memorySegment)
            throws Exception {
        return new CompactedLogWriteBatch(
                tb,
                DATA1_PHYSICAL_TABLE_PATH,
                MemoryLogRecordsCompactedBuilder.builder(
                        baseLogOffset,
                        DATA1_TABLE_INFO.getSchemaId(),
                        writeLimit,
                        (byte) 0,
                        new MemorySegmentOutputView(memorySegment)));
    }

    private void assertDefaultLogRecordBatchEquals(DefaultLogRecordBatch recordBatch) {
        assertThat(recordBatch.getRecordCount()).isEqualTo(1);
        assertThat(recordBatch.baseLogOffset()).isEqualTo(0L);
        assertThat(recordBatch.schemaId()).isEqualTo((short) DATA1_TABLE_INFO.getSchemaId());
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createCompactedReadContext(
                                DATA1_ROW_TYPE, DATA1_TABLE_INFO.getSchemaId());
                CloseableIterator<LogRecord> iterator = recordBatch.records(readContext)) {
            assertThat(iterator.hasNext()).isTrue();
            LogRecord record = iterator.next();
            assertThat(record.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
            assertThat(record.getRow()).isEqualTo(row);
            assertThat(iterator.hasNext()).isFalse();
        }
    }

    private WriteCallback newWriteCallback() {
        return exception -> {
            if (exception != null) {
                throw new RuntimeException(exception);
            }
        };
    }
}
