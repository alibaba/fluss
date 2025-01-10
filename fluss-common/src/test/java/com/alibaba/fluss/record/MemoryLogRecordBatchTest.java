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

import com.alibaba.fluss.row.TestInternalRowGenerator;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.testutils.DataTestUtils;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MemoryLogRecordBatch}. */
public class MemoryLogRecordBatchTest extends LogTestBase {

    @Test
    void testRecordBatchSize() throws Exception {
        MemoryLogRecords memoryLogRecords =
                DataTestUtils.genMemoryLogRecordsByObject(TestData.DATA1);
        int totalSize = 0;
        for (LogRecordBatch logRecordBatch : memoryLogRecords.batches()) {
            totalSize += logRecordBatch.sizeInBytes();
        }
        assertThat(totalSize).isEqualTo(memoryLogRecords.sizeInBytes());
    }

    @Test
    void testIndexedRowWriteAndReadBatch() throws Exception {
        int recordNumber = 50;
        RowType allRowType = TestInternalRowGenerator.createAllRowType();
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        baseLogOffset, schemaId, Integer.MAX_VALUE, magic, outputView);

        List<IndexedRow> rows = new ArrayList<>();
        for (int i = 0; i < recordNumber; i++) {
            IndexedRow row = TestInternalRowGenerator.genIndexedRowForAllType();
            builder.append(RowKind.INSERT, row);
            rows.add(row);
        }

        MemoryLogRecords memoryLogRecords = builder.build();
        Iterator<LogRecordBatch> iterator = memoryLogRecords.batches().iterator();

        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch logRecordBatch = iterator.next();

        logRecordBatch.ensureValid();

        assertThat(logRecordBatch.getRecordCount()).isEqualTo(recordNumber);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(baseLogOffset);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(baseLogOffset + recordNumber - 1);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(baseLogOffset + recordNumber);
        assertThat(logRecordBatch.magic()).isEqualTo(magic);
        assertThat(logRecordBatch.isValid()).isTrue();
        assertThat(logRecordBatch.schemaId()).isEqualTo(schemaId);

        // verify record.
        int i = 0;
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createIndexedReadContext(allRowType, schemaId);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            while (iter.hasNext()) {
                LogRecord record = iter.next();
                assertThat(record.logOffset()).isEqualTo(i);
                assertThat(record.getRowKind()).isEqualTo(RowKind.INSERT);
                assertThat(record.getRow()).isEqualTo(rows.get(i));
                i++;
            }
        }
    }
}
