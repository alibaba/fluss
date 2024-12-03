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

package com.alibaba.fluss.client.table;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.client.scanner.log.LogScan;
import com.alibaba.fluss.client.scanner.log.LogScanner;
import com.alibaba.fluss.client.scanner.snapshot.HybridScan;
import com.alibaba.fluss.client.scanner.snapshot.HybridScanner;
import com.alibaba.fluss.client.scanner.snapshot.SnapshotScan;
import com.alibaba.fluss.client.scanner.snapshot.SnapshotScanner;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWrite;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.row.InternalRow;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Used to communicate with a single Fluss table. Obtain an instance from a {@link Connection}.
 *
 * <p>Table can be used to get, put, delete or scan data from a fluss table.
 *
 * @since 0.1
 */
@PublicEvolving
public interface Table extends AutoCloseable {

    /**
     * Get the {@link TableDescriptor} for this table.
     *
     * <p>Note: the table info of this {@link Table} is set during the creation of this {@link
     * Table} and will not be updated after that, even if the table info of the table has been
     * changed. Therefore, if there are any changes to the table info, it may be necessary to
     * reconstruct the {@link Table}.
     */
    TableDescriptor getDescriptor();

    /**
     * Lookups certain row from the given table primary keys.
     *
     * @param key the given table primary keys.
     * @return the result of get.
     */
    CompletableFuture<LookupResult> lookup(InternalRow key);

    /**
     * Extracts limit number of rows from the given table bucket.
     *
     * @param tableBucket the target table bucket to scan.
     * @param limit the given limit number.
     * @param projectedFields the projection fields.
     * @return the result of get.
     */
    CompletableFuture<List<ScanRecord>> limitScan(
            TableBucket tableBucket, int limit, @Nullable int[] projectedFields);

    /**
     * Get a {@link AppendWriter} to write data to the table. Only available for Log Table. Will
     * throw exception when the table is a primary key table.
     *
     * @return the {@link AppendWriter} to write data to the table.
     */
    AppendWriter getAppendWriter();

    /**
     * Get a {@link UpsertWriter} to write data to the table. Only available for Primary Key Table.
     * Will throw exception when the table isn't a Primary Key Table.
     *
     * @return the {@link UpsertWriter} to write data to the table.
     */
    UpsertWriter getUpsertWriter(UpsertWrite upsertWrite);

    /**
     * Get a {@link UpsertWriter} to write data to the table. Only available for Primary Key Table.
     * Will throw exception when the table isn't a Primary Key Table.
     *
     * @return the {@link UpsertWriter} to write data to the table.
     */
    UpsertWriter getUpsertWriter();

    /**
     * Get a {@link LogScanner} to scan log data from this table.
     *
     * @return the {@link LogScanner} to scan log data from this table.
     */
    LogScanner getLogScanner(LogScan logScan);

    /**
     * Get a {@link SnapshotScanner} to scan data from this table according to provided {@link
     * SnapshotScan}.
     *
     * @return the {@link SnapshotScanner} to scan data from this table.
     */
    SnapshotScanner getSnapshotScanner(SnapshotScan snapshotScan);

    /**
     * Get a {@link HybridScanner} to scan data from this table according to provided {@link
     * HybridScan} and merge a piece of log.
     *
     * @return the {@link HybridScanner} to scan data from this table.
     */
    HybridScanner getHybridScanner(HybridScan hybridScan);
}
