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

package com.alibaba.fluss.server.kv.wal;

import com.alibaba.fluss.memory.MemorySegmentOutputView;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.record.MemoryLogRecordsCompactedBuilder;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.row.InternalRow;

import java.io.IOException;

/** A {@link WalBuilder} that builds a {@link MemoryLogRecords} with compacted log format. */
public class CompactedWalBuilder implements WalBuilder {

    private final MemoryLogRecordsCompactedBuilder recordsBuilder;

    public CompactedWalBuilder(int schemaId) throws IOException {
        this.recordsBuilder =
                MemoryLogRecordsCompactedBuilder.builder(
                        schemaId, new MemorySegmentOutputView(100));
    }

    @Override
    public void append(RowKind rowKind, InternalRow row) throws Exception {
        recordsBuilder.append(rowKind, row);
    }

    @Override
    public MemoryLogRecords build() throws Exception {
        recordsBuilder.close();
        return recordsBuilder.build();
    }

    @Override
    public void setWriterState(long writerId, int batchSequence) {
        recordsBuilder.setWriterState(writerId, batchSequence);
    }

    @Override
    public void deallocate() {
        // do nothing
    }
}
