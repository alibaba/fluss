/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.laketiering;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.client.write.HashBucketAssigner;
import com.alibaba.fluss.flink.source.split.LogSplit;
import com.alibaba.fluss.flink.source.split.SnapshotSplit;
import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.flink.utils.FlinkTestBase;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.encode.CompactedKeyEncoder;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** UT for {@link LakeTieringSplitReader}. */
class LakeTieringSplitReaderTest extends FlinkTestBase {

    @Test
    void testTieringReadOneTable() throws Exception {
        TablePath tablePath = LakeTieringSplitReader.DEFAULT_TABLE_PATH;
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        try (LakeTieringSplitReader<TestingWriteResult> tieringSplitReader =
                createTieringReader()) {
            // test empty splits
            SplitsAddition<SourceSplitBase> splitsAddition =
                    new SplitsAddition<>(
                            Arrays.asList(
                                    createLogSplit(tableId, 0, EARLIEST_OFFSET, 0),
                                    createLogSplit(tableId, 1, EARLIEST_OFFSET, 0),
                                    createLogSplit(tableId, 2, EARLIEST_OFFSET, 0)));
            tieringSplitReader.handleSplitsChanges(splitsAddition);

            tieringSplitReader.fetch();

            RecordsWithSplitIds<TableBucketWriteResult<TestingWriteResult>> fetchResult =
                    tieringSplitReader.fetch();
            for (int i = 0; i < 3; i++) {
                fetchResult.nextSplit();
                TableBucketWriteResult<TestingWriteResult> nextRecord =
                        fetchResult.nextRecordFromSplit();
                assertThat(nextRecord.writeResult()).isNull();
            }
            assertThat(fetchResult.nextSplit()).isNull();
            assertThat(fetchResult.finishedSplits())
                    .isEqualTo(
                            splitsAddition.splits().stream()
                                    .map(SourceSplitBase::splitId)
                                    .collect(Collectors.toSet()));

            // test snapshot splits
            // firstly, write some records into the table
            Map<TableBucket, List<InternalRow>> firstRows = putRows(tableId, tablePath, 10);

            // check the expected records
            waitUntilSnapshot(tableId, 0);

            splitsAddition =
                    new SplitsAddition<>(
                            Arrays.asList(
                                    createSnapshotSplit(tableId, 0, 0),
                                    createSnapshotSplit(tableId, 1, 0),
                                    createSnapshotSplit(tableId, 2, 0)));
            tieringSplitReader.handleSplitsChanges(splitsAddition);

            // fetch firstly to make snapshot splits as pending splits
            tieringSplitReader.fetch();
            for (int i = 0; i < 3; i++) {
                // one fetch to make tiering the snapshot records
                tieringSplitReader.fetch();
                // one fetch to make this snapshot split as finished
                fetchResult = tieringSplitReader.fetch();
                fetchResult.nextSplit();
                TableBucketWriteResult<TestingWriteResult> tableBucketWriteResult =
                        fetchResult.nextRecordFromSplit();
                int writeResult = tableBucketWriteResult.writeResult().getWriteResult();
                TableBucket tableBucket = tableBucketWriteResult.tableBucket();
                // check write result
                assertThat(writeResult).isEqualTo(firstRows.get(tableBucket).size());
            }

            // test log splits, should produce -U, +U for each record
            Map<TableBucket, List<InternalRow>> secondRows = putRows(tableId, tablePath, 10);
            List<SourceSplitBase> logSplits = new ArrayList<>();
            for (int bucket = 0; bucket < 3; bucket++) {
                TableBucket tableBucket = new TableBucket(tableId, bucket);
                long startingOffset = firstRows.get(tableBucket).size();
                long stoppingOffset = startingOffset + secondRows.get(tableBucket).size() * 2L;
                logSplits.add(new LogSplit(tableBucket, null, startingOffset, stoppingOffset));
            }
            tieringSplitReader.handleSplitsChanges(new SplitsAddition<>(logSplits));

            fetchResult = tieringSplitReader.fetch();
            for (int i = 0; i < 3; i++) {
                fetchResult.nextSplit();
                TableBucketWriteResult<TestingWriteResult> tableBucketWriteResult =
                        fetchResult.nextRecordFromSplit();
                TableBucket tableBucket = tableBucketWriteResult.tableBucket();
                assertThat(tableBucketWriteResult.writeResult().getWriteResult())
                        .isEqualTo(
                                // -U, +U
                                secondRows.get(tableBucket).size() * 2L);
            }

            // all finished, fetch again, should get empty
            fetchResult = tieringSplitReader.fetch();
            assertThat(fetchResult.nextSplit()).isNull();
        }
    }

    @Test
    @Disabled
    void testTieringReadMultipleTables() throws Exception {
        // todo: add real multiple table path
        TablePath tablePath = LakeTieringSplitReader.DEFAULT_TABLE_PATH;
        long tableId0 = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        long tableId1 = tableId0 + 1;

        try (LakeTieringSplitReader<TestingWriteResult> tieringSplitReader =
                createTieringReader()) {
            Map<TableBucket, List<InternalRow>> firstRows = putRows(tableId0, tablePath, 10);

            // first add bucket 0, bucket 1 of table id 0
            tieringSplitReader.handleSplitsChanges(
                    new SplitsAddition<>(
                            Arrays.asList(
                                    createLogSplit(
                                            tableId0,
                                            0,
                                            EARLIEST_OFFSET,
                                            firstRows.get(new TableBucket(tableId0, 0)).size()),
                                    createLogSplit(
                                            tableId0,
                                            1,
                                            EARLIEST_OFFSET,
                                            firstRows.get(new TableBucket(tableId0, 1)).size()))));

            // then add bucket0, bucket1, bucket2 of table id 1
            tieringSplitReader.handleSplitsChanges(
                    new SplitsAddition<>(
                            Arrays.asList(
                                    createLogSplit(
                                            tableId1,
                                            0,
                                            EARLIEST_OFFSET,
                                            firstRows.get(new TableBucket(tableId0, 0)).size()),
                                    createLogSplit(
                                            tableId1,
                                            1,
                                            EARLIEST_OFFSET,
                                            firstRows.get(new TableBucket(tableId0, 1)).size()),
                                    createLogSplit(
                                            tableId1,
                                            2,
                                            EARLIEST_OFFSET,
                                            firstRows.get(new TableBucket(tableId0, 2)).size()))));

            // add bucket2 of table id 0
            tieringSplitReader.handleSplitsChanges(
                    new SplitsAddition<>(
                            Collections.singletonList(
                                    createLogSplit(
                                            tableId0,
                                            2,
                                            EARLIEST_OFFSET,
                                            firstRows.get(new TableBucket(tableId0, 2)).size()))));

            // one fetch, it will be table id 0
            RecordsWithSplitIds<TableBucketWriteResult<TestingWriteResult>> fetchResult =
                    tieringSplitReader.fetch();
            for (int i = 0; i < 3; i++) {
                fetchResult.nextSplit();
                TableBucketWriteResult<TestingWriteResult> tableBucketWriteResult =
                        fetchResult.nextRecordFromSplit();
                TableBucket tableBucket = tableBucketWriteResult.tableBucket();
                assertThat(tableBucket.getTableId()).isEqualTo(tableId0);
                assertThat(tableBucketWriteResult.writeResult().getWriteResult())
                        .isEqualTo(firstRows.get(tableBucket).size());
            }

            //            // uncomment it when multiple tables are really supported
            //
            //            // another fetch, it will be table id 1
            //            fetchResult = tieringSplitReader.fetch();
            //            for (int i = 0; i < 3; i++) {
            //                fetchResult.nextSplit();
            //                TableBucketWriteResult<TestingWriteResult> tableBucketWriteResult =
            //                        fetchResult.nextRecordFromSplit();
            //                TableBucket tableBucket = tableBucketWriteResult.tableBucket();
            //                assertThat(tableBucket.getTableId()).isEqualTo(tableId1);
            //                assertThat(tableBucketWriteResult.writeResult().getWriteResult())
            //                        .isEqualTo(firstRows.get(tableBucket).size());
            //            }
            //
            //            // fetch again, should be empty
            //            fetchResult = tieringSplitReader.fetch();
            //            assertThat(fetchResult.nextSplit()).isNull();
        }
    }

    private LakeTieringSplitReader<TestingWriteResult> createTieringReader() {
        return new LakeTieringSplitReader<>(
                FLUSS_CLUSTER_EXTENSION.getClientConfig(), new TestingLakeTieringFactory());
    }

    private LogSplit createLogSplit(
            long tableId, int bucket, long startingOffset, long stoppingOffset) {
        TableBucket tableBucket = new TableBucket(tableId, bucket);
        return new LogSplit(tableBucket, null, startingOffset, stoppingOffset);
    }

    private SnapshotSplit createSnapshotSplit(long tableId, int bucket, long snapshotId) {
        TableBucket tableBucket = new TableBucket(tableId, bucket);
        return new SnapshotSplit(tableBucket, null, snapshotId) {
            @Override
            public String splitId() {
                return toSplitId("snapshot", tableBucket);
            }
        };
    }

    private Map<TableBucket, List<InternalRow>> putRows(long tableId, TablePath tablePath, int rows)
            throws Exception {
        Map<TableBucket, List<InternalRow>> rowsByBuckets = new HashMap<>();
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            for (int i = 0; i < rows; i++) {
                InternalRow row = row(i, "v" + i);
                upsertWriter.upsert(row);
                TableBucket tableBucket = new TableBucket(tableId, getBucketId(row));
                rowsByBuckets.computeIfAbsent(tableBucket, k -> new ArrayList<>()).add(row);
            }
            upsertWriter.flush();
        }
        return rowsByBuckets;
    }

    private static int getBucketId(InternalRow row) {
        CompactedKeyEncoder keyEncoder =
                new CompactedKeyEncoder(
                        DEFAULT_PK_TABLE_SCHEMA.getRowType(),
                        DEFAULT_PK_TABLE_SCHEMA.getPrimaryKeyIndexes());
        byte[] key = keyEncoder.encodeKey(row);
        HashBucketAssigner hashBucketAssigner = new HashBucketAssigner(DEFAULT_BUCKET_NUM);
        return hashBucketAssigner.assignBucket(key);
    }
}
