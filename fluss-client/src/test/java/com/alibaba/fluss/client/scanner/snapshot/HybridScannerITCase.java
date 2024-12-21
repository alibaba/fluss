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

import com.alibaba.fluss.client.admin.OffsetSpec;
import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.snapshot.BucketSnapshotInfo;
import com.alibaba.fluss.client.table.snapshot.BucketsSnapshotInfo;
import com.alibaba.fluss.client.table.snapshot.KvSnapshotInfo;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.ProjectedRow;
import com.alibaba.fluss.types.RowType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for {@link HybridScanner}. */
public class HybridScannerITCase extends SnapshotScannerITCase {
    private static final String DEFAULT_DB = "test-hybrid-scan-db";

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testScanHybrid(boolean hybridScanWithoutSnapshotFile) throws Exception {
        TablePath tablePath =
                TablePath.of(DEFAULT_DB, "test-table-hybrid-" + hybridScanWithoutSnapshotFile);
        long tableId = createTable(tablePath, DEFAULT_TABLE_DESCRIPTOR, true);

        // scan the snapshot
        Map<TableBucket, List<InternalRow>> expectedRowByBuckets = putRows(tableId, tablePath, 10);
        // test read snapshot without waiting snapshot finish.
        testHybridRead(tablePath, expectedRowByBuckets, hybridScanWithoutSnapshotFile, null);

        // test again with waiting snapshot finish.
        expectedRowByBuckets = putRows(tableId, tablePath, 20);
        waitUtilAllSnapshotFinished(expectedRowByBuckets.keySet(), 0);
        testHybridRead(tablePath, expectedRowByBuckets, hybridScanWithoutSnapshotFile, null);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testScanHybridWithProjection(boolean hybridScanWithoutSnapshotFile) throws Exception {
        TablePath tablePath =
                TablePath.of(
                        DEFAULT_DB,
                        "test-table-hybrid-projection-" + hybridScanWithoutSnapshotFile);
        long tableId = createTable(tablePath, DEFAULT_TABLE_DESCRIPTOR, true);

        // scan the snapshot
        Map<TableBucket, List<InternalRow>> expectedRowByBuckets = putRows(tableId, tablePath, 10);
        // test read snapshot without waiting snapshot finish.
        testHybridRead(
                tablePath, expectedRowByBuckets, hybridScanWithoutSnapshotFile, new int[] {0});
        // test again with waiting snapshot finish.
        expectedRowByBuckets = putRows(tableId, tablePath, 20);
        waitUtilAllSnapshotFinished(expectedRowByBuckets.keySet(), 0);
        testHybridRead(
                tablePath, expectedRowByBuckets, hybridScanWithoutSnapshotFile, new int[] {1});

        // test read snapshot again
        expectedRowByBuckets = putRows(tableId, tablePath, 20);
        testHybridRead(
                tablePath, expectedRowByBuckets, hybridScanWithoutSnapshotFile, new int[] {1, 0});
    }

    private void testHybridRead(
            TablePath tablePath,
            Map<TableBucket, List<InternalRow>> bucketRows,
            boolean hybridScanWithoutSnapshotFile,
            int[] projectedFields)
            throws Exception {
        KvSnapshotInfo tableSnapshotInfo = admin.getKvSnapshot(tablePath).get();
        Map<Integer, Long> latestOffsets =
                admin.listOffsets(
                                PhysicalTablePath.of(tablePath, null),
                                bucketRows.keySet().stream()
                                        .map(TableBucket::getBucket)
                                        .collect(Collectors.toSet()),
                                new OffsetSpec.LatestSpec())
                        .all()
                        .get();

        BucketsSnapshotInfo bucketsSnapshotInfo = tableSnapshotInfo.getBucketsSnapshots();
        long tableId = tableSnapshotInfo.getTableId();
        try (Table table = conn.getTable(tablePath)) {
            for (int bucketId : bucketsSnapshotInfo.getBucketIds()) {
                TableBucket tableBucket = new TableBucket(tableId, bucketId);
                Optional<BucketSnapshotInfo> bucketSnapshotInfo =
                        bucketsSnapshotInfo.getBucketSnapshotInfo(bucketId);
                // get the expected rows
                List<InternalRow> expectedRows = bucketRows.get(tableBucket);
                if (projectedFields != null) {
                    expectedRows =
                            expectedRows.stream()
                                    .map(row -> ProjectedRow.from(projectedFields).replaceRow(row))
                                    .collect(Collectors.toList());
                }

                // create the hybrid scan according to the snapshot files
                HybridScan hybridScan =
                        new HybridScan(
                                tableBucket,
                                bucketSnapshotInfo.isPresent() && !hybridScanWithoutSnapshotFile
                                        ? bucketSnapshotInfo.get().getSnapshotFiles()
                                        : Collections.emptyList(),
                                DEFAULT_SCHEMA,
                                projectedFields,
                                bucketSnapshotInfo.isPresent() && !hybridScanWithoutSnapshotFile
                                        ? bucketSnapshotInfo.get().getLogOffset()
                                        : -2,
                                latestOffsets.get(bucketId));
                HybridScanner hybridScanner = table.getHybridScanner(hybridScan);

                // collect all the records from the scanner
                List<ScanRecord> scanRecords = collectRecords(hybridScanner);

                // check the records
                if (projectedFields == null) {
                    assertScanRecords(scanRecords, expectedRows);
                } else {
                    InternalRow.FieldGetter[] fieldGetters =
                            new InternalRow.FieldGetter[projectedFields.length];
                    RowType rowType = admin.getTableSchema(tablePath).get().getSchema().toRowType();
                    for (int i = 0; i < projectedFields.length; i++) {
                        fieldGetters[i] =
                                InternalRow.createFieldGetter(
                                        rowType.getTypeAt(projectedFields[i]), i);
                    }
                    assertScanRecords(scanRecords, expectedRows, fieldGetters);
                }
            }
        }
    }

    private void assertScanRecords(
            List<ScanRecord> actualScanRecords,
            List<InternalRow> expectRows,
            InternalRow.FieldGetter[] fieldGetters) {
        List<ScanRecord> expectedScanRecords = new ArrayList<>(expectRows.size());
        // Transform to GenericRow for comparing value rather than object.
        actualScanRecords =
                actualScanRecords.stream()
                        .map(
                                record -> {
                                    GenericRow genericRow = new GenericRow(fieldGetters.length);
                                    for (int i = 0; i < fieldGetters.length; i++) {
                                        genericRow.setField(
                                                i, fieldGetters[i].getFieldOrNull(record.getRow()));
                                    }
                                    return new ScanRecord(
                                            record.getOffset(),
                                            record.getTimestamp(),
                                            record.getRowKind(),
                                            genericRow);
                                })
                        .collect(Collectors.toList());
        for (InternalRow row : expectRows) {
            GenericRow genericRow = new GenericRow(fieldGetters.length);
            for (int i = 0; i < fieldGetters.length; i++) {
                genericRow.setField(i, fieldGetters[i].getFieldOrNull(row));
            }

            expectedScanRecords.add(new ScanRecord(genericRow));
        }

        assertThat(actualScanRecords).containsExactlyInAnyOrderElementsOf(expectedScanRecords);
    }
}
