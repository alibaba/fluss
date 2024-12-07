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

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.compacted.CompactedRow;
import com.alibaba.fluss.row.encode.CompactedRowEncoder;
import com.alibaba.fluss.types.DataType;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.alibaba.fluss.row.TestInternalRowGenerator.createAllRowType;
import static com.alibaba.fluss.row.TestInternalRowGenerator.genCompactedRowForAllType;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CompactedLogRecord}. */
public class CompactedLogRecordTest extends LogTestBase {

    @Test
    void testBase() throws IOException {
        DataType[] fieldTypes = baseRowType.getChildren().toArray(new DataType[0]);
        // create row.
        CompactedRowEncoder encoder = new CompactedRowEncoder(fieldTypes);
        encoder.startNewRow();
        encoder.encodeField(0, 10);
        encoder.encodeField(1, BinaryString.fromString("abc"));
        CompactedRow compactedRow = encoder.finishRow();
        CompactedLogRecord.writeTo(outputView, RowKind.APPEND_ONLY, compactedRow);
        // Test read from.
        CompactedLogRecord compactedLogRecord =
                CompactedLogRecord.readFrom(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()),
                        0,
                        1000,
                        10001,
                        fieldTypes);
        assertThat(compactedLogRecord.getSizeInBytes()).isEqualTo(11);
        assertThat(compactedLogRecord.logOffset()).isEqualTo(1000);
        assertThat(compactedLogRecord.timestamp()).isEqualTo(10001);
        assertThat(compactedLogRecord.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
        assertThat(compactedLogRecord.getRow()).isEqualTo(compactedRow);
    }

    @Test
    void testWriteToAndReadFromWithRandomData() throws IOException {
        // Test write to.
        CompactedRow row = genCompactedRowForAllType();
        CompactedLogRecord.writeTo(outputView, RowKind.APPEND_ONLY, row);
        DataType[] allColTypes = createAllRowType().getChildren().toArray(new DataType[0]);
        // Test read from.
        CompactedLogRecord compactedLogRecord =
                CompactedLogRecord.readFrom(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()),
                        0,
                        1000,
                        10001,
                        allColTypes);
        assertThat(compactedLogRecord.logOffset()).isEqualTo(1000);
        assertThat(compactedLogRecord.timestamp()).isEqualTo(10001);
        assertThat(compactedLogRecord.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
    }
}
