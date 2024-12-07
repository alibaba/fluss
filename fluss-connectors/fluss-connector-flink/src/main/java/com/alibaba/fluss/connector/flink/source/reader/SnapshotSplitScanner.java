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

package com.alibaba.fluss.connector.flink.source.reader;

import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.client.scanner.snapshot.HybridScan;
import com.alibaba.fluss.client.scanner.snapshot.SnapshotScan;
import com.alibaba.fluss.client.scanner.snapshot.SnapshotScanner;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.connector.flink.source.split.HybridSnapshotLogSplit;
import com.alibaba.fluss.connector.flink.source.split.SnapshotSplit;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;

/** A scanner for {@link SnapshotSplit}. */
public class SnapshotSplitScanner implements SplitScanner {

    private final SnapshotScanner snapshotScanner;

    private SnapshotSplitScanner(SnapshotScanner scanner) {
        this.snapshotScanner = scanner;
    }

    public static SnapshotSplitScanner ofSnapshotScanner(
            Table table, @Nullable int[] projectedFields, SnapshotSplit snapshotSplit) {
        Schema tableSchema = table.getDescriptor().getSchema();
        SnapshotScan snapshotScan =
                new SnapshotScan(
                        snapshotSplit.getTableBucket(),
                        snapshotSplit.getSnapshotFiles(),
                        tableSchema,
                        projectedFields);
        return new SnapshotSplitScanner(table.getSnapshotScanner(snapshotScan));
    }

    public static SnapshotSplitScanner ofHybridScanner(
            Table table,
            @Nullable int[] projectedFields,
            HybridSnapshotLogSplit hybridSnapshotLogSplit) {
        if (!hybridSnapshotLogSplit.getLogStoppingOffset().isPresent()) {
            throw new IllegalArgumentException(
                    "Stopping offset must greater than 0 for batch read.");
        }
        Schema tableSchema = table.getDescriptor().getSchema();
        HybridScan hybridScan =
                new HybridScan(
                        hybridSnapshotLogSplit.getTableBucket(),
                        hybridSnapshotLogSplit.getSnapshotFiles(),
                        tableSchema,
                        projectedFields,
                        hybridSnapshotLogSplit.getLogStartingOffset(),
                        hybridSnapshotLogSplit.getLogStoppingOffset().get());
        return new SnapshotSplitScanner(table.getHybridScanner(hybridScan));
    }

    @Nullable
    @Override
    public CloseableIterator<ScanRecord> poll(Duration timeout) throws IOException {
        return snapshotScanner.poll(timeout);
    }

    @Override
    public void close() throws Exception {
        snapshotScanner.close();
    }
}
