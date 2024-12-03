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

import com.alibaba.fluss.client.admin.ClientToServerITCaseBase;
import com.alibaba.fluss.client.scanner.RemoteFileDownloader;
import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.snapshot.BucketSnapshotInfo;
import com.alibaba.fluss.client.table.snapshot.BucketsSnapshotInfo;
import com.alibaba.fluss.client.table.snapshot.KvSnapshotInfo;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.client.write.HashBucketAssigner;
import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for {@link SnapshotScanner}. */
class SnapshotScannerITCase extends ClientToServerITCaseBase {

    private static final int DEFAULT_BUCKET_NUM = 3;

    protected static final Schema DEFAULT_SCHEMA =
            Schema.newBuilder()
                    .primaryKey("id")
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .build();

    protected static final TableDescriptor DEFAULT_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(DEFAULT_SCHEMA)
                    .distributedBy(DEFAULT_BUCKET_NUM, "id")
                    .build();

    private static final KeyEncoder DEFAULT_KEY_ENCODER =
            new KeyEncoder(DEFAULT_SCHEMA.toRowType(), DEFAULT_SCHEMA.getPrimaryKeyIndexes());

    private static final HashBucketAssigner DEFAULT_BUCKET_ASSIGNER =
            new HashBucketAssigner(DEFAULT_BUCKET_NUM);

    private static final String DEFAULT_DB = "test-snapshot-scan-db";

    private RemoteFileDownloader remoteFileDownloader;

    @BeforeEach
    protected void setup() throws Exception {
        super.setup();
        remoteFileDownloader = new RemoteFileDownloader(1);
    }

    @AfterEach
    protected void teardown() throws Exception {
        if (remoteFileDownloader != null) {
            remoteFileDownloader.close();
            remoteFileDownloader = null;
        }
        super.teardown();
    }

    @Test
    void testScanSnapshot() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test-table-snapshot");
        long tableId = createTable(tablePath, DEFAULT_TABLE_DESCRIPTOR, true);

        // scan the snapshot
        Map<TableBucket, List<InternalRow>> expectedRowByBuckets = putRows(tableId, tablePath, 10);

        // wait snapshot finish
        waitUtilAllSnapshotFinished(expectedRowByBuckets.keySet(), 0);

        // test read snapshot
        testSnapshotRead(tablePath, expectedRowByBuckets);

        // test again;
        expectedRowByBuckets = putRows(tableId, tablePath, 20);

        // wait snapshot finish
        waitUtilAllSnapshotFinished(expectedRowByBuckets.keySet(), 1);

        // test read snapshot
        testSnapshotRead(tablePath, expectedRowByBuckets);
    }

    protected Map<TableBucket, List<InternalRow>> putRows(
            long tableId, TablePath tablePath, int rows) throws Exception {
        Map<TableBucket, List<InternalRow>> rowsByBuckets = new HashMap<>();
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.getUpsertWriter();
            for (int i = 0; i < rows; i++) {
                InternalRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {i, "v" + i});
                upsertWriter.upsert(row);
                TableBucket tableBucket = new TableBucket(tableId, getBucketId(row));
                rowsByBuckets.computeIfAbsent(tableBucket, k -> new ArrayList<>()).add(row);
            }
            upsertWriter.flush();
        }
        return rowsByBuckets;
    }

    private void testSnapshotRead(
            TablePath tablePath, Map<TableBucket, List<InternalRow>> bucketRows) throws Exception {
        KvSnapshotInfo kvSnapshotInfo = admin.getKvSnapshot(tablePath).get();
        BucketsSnapshotInfo bucketsSnapshotInfo = kvSnapshotInfo.getBucketsSnapshots();
        long tableId = kvSnapshotInfo.getTableId();
        try (Table table = conn.getTable(tablePath)) {
            for (int bucketId : bucketsSnapshotInfo.getBucketIds()) {
                TableBucket tableBucket = new TableBucket(tableId, bucketId);
                BucketSnapshotInfo bucketSnapshotInfo =
                        bucketsSnapshotInfo.getBucketSnapshotInfo(bucketId).get();

                // create the snapshot scan according to the snapshot files
                SnapshotScan snapshotScan =
                        new SnapshotScan(
                                tableBucket,
                                bucketSnapshotInfo.getSnapshotFiles(),
                                DEFAULT_SCHEMA,
                                null);
                SnapshotScanner snapshotScanner = table.getSnapshotScanner(snapshotScan);

                // collect all the records from the scanner
                List<ScanRecord> scanRecords = collectRecords(snapshotScanner);

                // get the expected rows
                List<InternalRow> expectedRows = bucketRows.get(tableBucket);

                // check the records
                assertScanRecords(scanRecords, expectedRows);
            }
        }
    }

    // -------- Utils method

    private static int getBucketId(InternalRow row) {
        byte[] key = DEFAULT_KEY_ENCODER.encode(row);
        return DEFAULT_BUCKET_ASSIGNER.assignBucket(key, Cluster.empty());
    }

    protected void waitUtilAllSnapshotFinished(Set<TableBucket> tableBuckets, long snapshotId) {
        for (TableBucket tableBucket : tableBuckets) {
            FLUSS_CLUSTER_EXTENSION.waitUtilSnapshotFinished(tableBucket, snapshotId);
        }
    }

    protected List<ScanRecord> collectRecords(SnapshotScanner snapshotScanner) {
        List<ScanRecord> scanRecords = new ArrayList<>();
        Iterator<ScanRecord> recordIterator = snapshotScanner.poll(Duration.ofSeconds(10));
        while (recordIterator != null) {
            while (recordIterator.hasNext()) {
                ScanRecord record = recordIterator.next();
                scanRecords.add(record);
            }
            recordIterator = snapshotScanner.poll(Duration.ofSeconds(10));
        }
        snapshotScanner.close();
        return scanRecords;
    }

    protected void assertScanRecords(
            List<ScanRecord> actualScanRecords, List<InternalRow> expectRows) {
        List<ScanRecord> expectedScanRecords = new ArrayList<>(expectRows.size());
        for (InternalRow row : expectRows) {
            expectedScanRecords.add(new ScanRecord(row));
        }
        assertThat(actualScanRecords).containsExactlyInAnyOrderElementsOf(expectedScanRecords);
    }
}
