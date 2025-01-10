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

import com.alibaba.fluss.client.metrics.TestingWriterMetricGroup;
import com.alibaba.fluss.cluster.BucketLocation;
import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.DefaultKvRecord;
import com.alibaba.fluss.record.DefaultLogRecord;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.record.LogRecordBatch;
import com.alibaba.fluss.record.LogRecordReadContext;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.metrics.TestingClientMetricGroup;
import com.alibaba.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.fluss.record.MemoryLogRecordBatch.RECORD_BATCH_HEADER_SIZE;
import static com.alibaba.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RecordAccumulator}. */
public class RecordAccumulatorTest {
    ServerNode node1 = new ServerNode(1, "localhost", 90, ServerType.TABLET_SERVER);
    ServerNode node2 = new ServerNode(2, "localhost", 91, ServerType.TABLET_SERVER);
    ServerNode node3 = new ServerNode(3, "localhost", 92, ServerType.TABLET_SERVER);
    private final ServerNode[] serverNodes = new ServerNode[] {node1, node2, node3};
    private final TableBucket tb1 = new TableBucket(DATA1_TABLE_ID, 0);
    private final TableBucket tb2 = new TableBucket(DATA1_TABLE_ID, 1);
    private final TableBucket tb3 = new TableBucket(DATA1_TABLE_ID, 2);
    private final TableBucket tb4 = new TableBucket(DATA1_TABLE_ID, 3);
    private final BucketLocation bucket1 =
            new BucketLocation(DATA1_PHYSICAL_TABLE_PATH, DATA1_TABLE_ID, 0, node1, serverNodes);
    private final BucketLocation bucket2 =
            new BucketLocation(DATA1_PHYSICAL_TABLE_PATH, DATA1_TABLE_ID, 1, node1, serverNodes);
    private final BucketLocation bucket3 =
            new BucketLocation(DATA1_PHYSICAL_TABLE_PATH, DATA1_TABLE_ID, 2, node2, serverNodes);
    private final BucketLocation bucket4 =
            new BucketLocation(DATA1_PHYSICAL_TABLE_PATH, DATA1_TABLE_ID, 3, node2, serverNodes);
    private final WriteCallback writeCallback =
            exception -> {
                if (exception != null) {
                    throw new RuntimeException(exception);
                }
            };

    private Configuration conf;
    private Cluster cluster;

    @BeforeEach
    public void start() {
        conf = new Configuration();
        // init cluster.
        cluster = updateCluster(Arrays.asList(bucket1, bucket2, bucket3));
    }

    // TODO Add more tests to test lingMs, retryBackoffMs, deliveryTimeoutMs and
    //  nextBatchExpiryTimeMs if we introduced.

    @Test
    void testDrainBatches() throws Exception {
        // test case: node1(tb1, tb2), node2(tb3).
        IndexedRow row = row(DATA1_ROW_TYPE, new Object[] {1, "a"});
        long batchSize = getTestBatchSize(row);
        RecordAccumulator accum = createTestRecordAccumulator((int) batchSize, Integer.MAX_VALUE);

        // add bucket into cluster.
        cluster = updateCluster(Arrays.asList(bucket1, bucket2, bucket3, bucket4));

        // initial data.
        for (int i = 0; i < 4; i++) {
            accum.append(createRecord(row), writeCallback, cluster, i, false);
        }

        // drain batches from 2 nodes: node1 => tb1, node2 => tb3, because the max request size is
        // full after the first batch drained
        Map<Integer, List<WriteBatch>> batches1 =
                accum.drain(cluster, new HashSet<>(Arrays.asList(node1, node2)), (int) batchSize);
        verifyTableBucketInBatches(batches1, tb1, tb3);

        // add record for tb1, tb3
        accum.append(createRecord(row), writeCallback, cluster, 0, false);
        accum.append(createRecord(row), writeCallback, cluster, 2, false);

        // drain batches from 2 nodes: node1 => tb2, node2 => tb4, because the max request size is
        // full after the first batch drained. The drain index should start from next table bucket,
        // that is, node1 => tb2, node2 => tb4
        Map<Integer, List<WriteBatch>> batches2 =
                accum.drain(cluster, new HashSet<>(Arrays.asList(node1, node2)), (int) batchSize);
        verifyTableBucketInBatches(batches2, tb2, tb4);

        // make sure in next run, the drain index will start from the beginning.
        Map<Integer, List<WriteBatch>> batches3 =
                accum.drain(cluster, new HashSet<>(Arrays.asList(node1, node2)), (int) batchSize);
        verifyTableBucketInBatches(batches3, tb1, tb3);
    }

    @Test
    void testFull() throws Exception {
        // test case assumes that the records do not fill the batch completely
        int batchSize = 1025;
        IndexedRow row = row(DATA1_ROW_TYPE, new Object[] {1, "a"});

        RecordAccumulator accum = createTestRecordAccumulator(batchSize, 10L * batchSize);
        int appends = expectedNumAppends(row, batchSize);
        for (int i = 0; i < appends; i++) {
            // append to the first batch
            accum.append(createRecord(row), writeCallback, cluster, 0, false);
            Deque<WriteBatch> writeBatches = accum.getDeque(DATA1_PHYSICAL_TABLE_PATH, tb1);
            assertThat(writeBatches).hasSize(1);

            WriteBatch batch = writeBatches.peekFirst();
            assertThat(batch.isClosed()).isFalse();
            // No buckets should be ready.
            assertThat(accum.ready(cluster).readyNodes.size()).isEqualTo(0);
        }

        // this appends doesn't fit in the first batch, so a new batch is created and the first
        // batch is closed.
        accum.append(createRecord(row), writeCallback, cluster, 0, false);
        Deque<WriteBatch> writeBatches = accum.getDeque(DATA1_PHYSICAL_TABLE_PATH, tb1);
        assertThat(writeBatches).hasSize(2);
        Iterator<WriteBatch> bucketBatchesIterator = writeBatches.iterator();
        assertThat(bucketBatchesIterator.next().isClosed()).isTrue();
        // Bucket's leader should be ready.
        assertThat(accum.ready(cluster).readyNodes).isEqualTo(Collections.singleton(node1));

        List<WriteBatch> batches =
                accum.drain(cluster, Collections.singleton(node1), Integer.MAX_VALUE)
                        .get(node1.id());
        assertThat(batches.size()).isEqualTo(1);
        WriteBatch batch = batches.get(0);
        assertThat(batch).isInstanceOf(IndexedLogWriteBatch.class);

        IndexedLogWriteBatch logProducerBatch = (IndexedLogWriteBatch) batch;

        MemoryLogRecords memoryLogRecords = logProducerBatch.records();
        Iterator<LogRecordBatch> iterator = memoryLogRecords.batches().iterator();
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createIndexedReadContext(
                                DATA1_ROW_TYPE, DATA1_TABLE_INFO.getSchemaId());
                CloseableIterator<LogRecord> iter = iterator.next().records(readContext)) {
            for (int i = 0; i < appends; i++) {
                LogRecord record = iter.next();
                assertThat(record.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
                assertThat(record.getRow()).isEqualTo(row);
            }
            assertThat(iter.hasNext()).isFalse();
        }
    }

    @Test
    void testAppendLarge() throws Exception {
        int batchSize = 10;
        // set batch timeout as 0 to make sure batch are always ready.
        RecordAccumulator accum = createTestRecordAccumulator(0, batchSize, 10L * 1024);

        InternalRow row1 =
                row(
                        DATA1_ROW_TYPE,
                        new Object[] {
                            100000000, "this is very large string which large that 10 bytes"
                        });
        // row size > 10;
        accum.append(createRecord(row1), writeCallback, cluster, 0, false);
        // bucket's leader should be ready for bucket0.
        assertThat(accum.ready(cluster).readyNodes).isEqualTo(Collections.singleton(node1));

        Deque<WriteBatch> writeBatches = accum.getDeque(DATA1_PHYSICAL_TABLE_PATH, tb1);
        assertThat(writeBatches).hasSize(1);
        WriteBatch batch = writeBatches.peek();
        assertThat(batch).isInstanceOf(IndexedLogWriteBatch.class);
        IndexedLogWriteBatch logProducerBatch = (IndexedLogWriteBatch) batch;
        assertThat(logProducerBatch).isNotNull();
        MemoryLogRecords memoryLogRecords = logProducerBatch.records();
        Iterator<? extends LogRecordBatch> iterator = memoryLogRecords.batches().iterator();
        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch logRecordBatch = iterator.next();
        try (LogRecordReadContext readContext =
                LogRecordReadContext.createIndexedReadContext(
                        DATA1_ROW_TYPE, DATA1_TABLE_INFO.getSchemaId())) {
            LogRecord record = logRecordBatch.records(readContext).next();
            assertThat(record.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
            assertThat(record.logOffset()).isEqualTo(0L);
            assertThat(record.getRow()).isEqualTo(row1);
        }
    }

    @Test
    void testAppendWithStickyBucketAssigner() throws Exception {
        // Test case assumes that the records do not fill the batch completely
        int batchSize = 100;
        IndexedRow row = row(DATA1_ROW_TYPE, new Object[] {1, "a"});

        StickyBucketAssigner bucketAssigner = new StickyBucketAssigner(DATA1_PHYSICAL_TABLE_PATH);
        RecordAccumulator accum =
                createTestRecordAccumulator(
                        (int) Duration.ofMinutes(1).toMillis(), batchSize, 10L * batchSize);
        int expectedAppends = expectedNumAppends(row, batchSize);

        // Create first batch.
        int bucketId = bucketAssigner.assignBucket(null, cluster);
        accum.append(createRecord(row), writeCallback, cluster, bucketId, false);
        int appends = 1;

        boolean switchBucket = false;
        while (!switchBucket) {
            // Append to the first batch.
            bucketId = bucketAssigner.assignBucket(null, cluster);
            RecordAccumulator.RecordAppendResult result =
                    accum.append(createRecord(row), writeCallback, cluster, bucketId, true);
            int numBatches = getBatchNumInAccum(accum);
            // Only one batch is created because the bucket is sticky.
            assertThat(numBatches).isEqualTo(1);

            switchBucket = result.abortRecordForNewBatch;
            // We only appended if we do not retry.
            if (!switchBucket) {
                appends++;
                assertThat(accum.ready(cluster).readyNodes.size()).isEqualTo(0);
            }
        }

        // Batch should be full.
        assertThat(accum.ready(cluster).readyNodes.size()).isEqualTo(1);
        assertThat(appends).isEqualTo(expectedAppends);
        switchBucket = false;

        // Writer would call this method in this case, make second batch.
        bucketAssigner.onNewBatch(cluster, bucketId);
        bucketId = bucketAssigner.assignBucket(null, cluster);
        accum.append(createRecord(row), writeCallback, cluster, bucketId, false);
        appends++;

        // These append operations all go into the second batch.
        while (!switchBucket) {
            // Append to the first batch.
            bucketId = bucketAssigner.assignBucket(null, cluster);
            RecordAccumulator.RecordAppendResult result =
                    accum.append(createRecord(row), writeCallback, cluster, bucketId, true);
            int numBatches = getBatchNumInAccum(accum);
            // Only one batch is created because the bucket is sticky.
            assertThat(numBatches).isEqualTo(2);

            switchBucket = result.abortRecordForNewBatch;
            // We only appended if we do not retry.
            if (!switchBucket) {
                appends++;
            }
        }

        // There should be two full batches now.
        assertThat(appends).isEqualTo(2 * expectedAppends);
    }

    @Test
    void testPartialDrain() throws Exception {
        IndexedRow row = row(DATA1_ROW_TYPE, new Object[] {1, "a"});
        RecordAccumulator accum = createTestRecordAccumulator(1024, 10L * 1024);
        int appends = 1024 / DefaultLogRecord.sizeOf(row) + 1;
        List<TableBucket> buckets = Arrays.asList(tb1, tb2);
        for (TableBucket tb : buckets) {
            for (int i = 0; i < appends; i++) {
                accum.append(createRecord(row), writeCallback, cluster, tb.getBucket(), false);
            }
        }

        assertThat(accum.ready(cluster).readyNodes).isEqualTo(Collections.singleton(node1));
        List<WriteBatch> batches =
                accum.drain(cluster, Collections.singleton(node1), 1024).get(node1.id());
        // Due to size bound only one bucket should have been retrieved.
        assertThat(batches.size()).isEqualTo(1);
    }

    @Test
    void testFlush() throws Exception {
        IndexedRow row = row(DATA1_ROW_TYPE, new Object[] {1, "a"});
        RecordAccumulator accum = createTestRecordAccumulator(4 * 1024, 64 * 1024);

        for (int i = 0; i < 100; i++) {
            accum.append(createRecord(row), writeCallback, cluster, i % 3, false);
            assertThat(accum.hasIncomplete()).isTrue();
        }

        assertThat(accum.ready(cluster).readyNodes.size()).isEqualTo(0);

        accum.beginFlush();
        // drain and deallocate all batches.
        Map<Integer, List<WriteBatch>> results =
                accum.drain(cluster, accum.ready(cluster).readyNodes, Integer.MAX_VALUE);
        assertThat(accum.hasIncomplete()).isTrue();

        for (List<WriteBatch> batches : results.values()) {
            for (WriteBatch batch : batches) {
                accum.deallocate(batch);
            }
        }

        // should be complete with no unsent records.
        accum.awaitFlushCompletion();
        assertThat(accum.hasUnDrained()).isFalse();
        assertThat(accum.hasIncomplete()).isFalse();
    }

    @Test
    void testTableWithUnknownLeader() throws Exception {
        int batchSize = 10;
        // set batch timeout as 0 to make sure batch are always ready.
        RecordAccumulator accum = createTestRecordAccumulator(0, batchSize, 10L * 1024);
        IndexedRow row = row(DATA1_ROW_TYPE, new Object[] {1, "a"});

        BucketLocation bucket1 =
                new BucketLocation(DATA1_PHYSICAL_TABLE_PATH, DATA1_TABLE_ID, 0, null, serverNodes);
        // add bucket1 which leader is unknown into cluster.
        cluster = updateCluster(Collections.singletonList(bucket1));

        accum.append(createRecord(row), writeCallback, cluster, 0, false);
        RecordAccumulator.ReadyCheckResult readyCheckResult = accum.ready(cluster);
        assertThat(readyCheckResult.unknownLeaderTables)
                .isEqualTo(Collections.singleton(DATA1_PHYSICAL_TABLE_PATH));
        assertThat(readyCheckResult.readyNodes.size()).isEqualTo(0);

        bucket1 =
                new BucketLocation(
                        DATA1_PHYSICAL_TABLE_PATH, DATA1_TABLE_ID, 0, node1, serverNodes);
        // update the bucket info with leader.
        cluster = updateCluster(Collections.singletonList(bucket1));

        readyCheckResult = accum.ready(cluster);
        assertThat(readyCheckResult.unknownLeaderTables).isEmpty();
        assertThat(readyCheckResult.readyNodes.size()).isEqualTo(1);
    }

    @Test
    void testAwaitFlushComplete() throws Exception {
        IndexedRow row = row(DATA1_ROW_TYPE, new Object[] {1, "a"});
        RecordAccumulator accum = createTestRecordAccumulator(4 * 1024, 64 * 1024);
        accum.append(createRecord(row), writeCallback, cluster, 0, false);

        accum.beginFlush();
        assertThat(accum.flushInProgress()).isTrue();
        delayedInterrupt(Thread.currentThread(), 1000L);
        assertThatThrownBy(accum::awaitFlushCompletion).isInstanceOf(InterruptedException.class);
    }

    private WriteRecord createRecord(InternalRow row) {
        return new WriteRecord(DATA1_PHYSICAL_TABLE_PATH, WriteKind.APPEND, row, null);
    }

    private Cluster updateCluster(List<BucketLocation> bucketLocations) {
        Map<Integer, ServerNode> aliveTabletServersById =
                Arrays.stream(serverNodes)
                        .collect(Collectors.toMap(ServerNode::id, serverNode -> serverNode));

        Map<PhysicalTablePath, List<BucketLocation>> bucketsByPath = new HashMap<>();
        bucketsByPath.put(DATA1_PHYSICAL_TABLE_PATH, bucketLocations);

        Map<TablePath, Long> tableIdByPath = new HashMap<>();
        tableIdByPath.put(DATA1_TABLE_PATH, DATA1_TABLE_ID);

        TableInfo data1NonPkTableInfo =
                new TableInfo(
                        DATA1_TABLE_PATH,
                        DATA1_TABLE_ID,
                        TableDescriptor.builder()
                                // use INDEXED format better memory control
                                // to test RecordAccumulator
                                .logFormat(LogFormat.INDEXED)
                                .schema(DATA1_SCHEMA)
                                .distributedBy(3)
                                .build(),
                        1);
        Map<TablePath, TableInfo> tableInfoByPath = new HashMap<>();
        tableInfoByPath.put(DATA1_TABLE_PATH, data1NonPkTableInfo);

        return new Cluster(
                aliveTabletServersById,
                new ServerNode(-1, "localhost", 89, ServerType.COORDINATOR),
                bucketsByPath,
                tableIdByPath,
                Collections.emptyMap(),
                tableInfoByPath);
    }

    private void delayedInterrupt(final Thread thread, final long delayMs) {
        Thread t =
                new Thread(
                        () -> {
                            try {
                                Thread.sleep(delayMs);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            thread.interrupt();
                        });
        t.start();
    }

    private void verifyTableBucketInBatches(
            Map<Integer, List<WriteBatch>> nodeBatches, TableBucket... tb) {
        List<TableBucket> tableBucketsInBatch = new ArrayList<>();
        nodeBatches.forEach(
                (bucket, batches) -> {
                    List<TableBucket> tbList =
                            batches.stream()
                                    .map(WriteBatch::tableBucket)
                                    .collect(Collectors.toList());
                    tableBucketsInBatch.addAll(tbList);
                });
        assertThat(tableBucketsInBatch).containsExactlyInAnyOrder(tb);
    }

    /** Return the offset delta. */
    private int expectedNumAppends(IndexedRow row, int batchSize) {
        int size = RECORD_BATCH_HEADER_SIZE;
        int offsetDelta = 0;
        while (true) {
            int recordSize = DefaultLogRecord.sizeOf(row);
            if (size + recordSize > batchSize) {
                return offsetDelta;
            }
            offsetDelta += 1;
            size += recordSize;
        }
    }

    private RecordAccumulator createTestRecordAccumulator(int batchSize, long totalSize) {
        return createTestRecordAccumulator(5000, batchSize, totalSize);
    }

    private RecordAccumulator createTestRecordAccumulator(
            int batchTimeoutMs, int batchSize, long totalSize) {
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT, Duration.ofMillis(batchTimeoutMs));
        // TODO client writer buffer maybe removed.
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, new MemorySize(totalSize));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, new MemorySize(batchSize));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, new MemorySize(batchSize));
        return new RecordAccumulator(
                conf,
                new IdempotenceManager(
                        false,
                        conf.getInt(ConfigOptions.CLIENT_WRITER_MAX_INFLIGHT_REQUESTS_PER_BUCKET),
                        GatewayClientProxy.createGatewayProxy(
                                () -> cluster.getRandomTabletServer(),
                                RpcClient.create(conf, TestingClientMetricGroup.newInstance()),
                                TabletServerGateway.class)),
                TestingWriterMetricGroup.newInstance());
    }

    private long getTestBatchSize(InternalRow row) {
        return RECORD_BATCH_HEADER_SIZE + DefaultKvRecord.sizeOf(new byte[4], row);
    }

    private int getBatchNumInAccum(RecordAccumulator accum) {
        Deque<WriteBatch> bucketBatches1 = accum.getDeque(DATA1_PHYSICAL_TABLE_PATH, tb1);
        Deque<WriteBatch> bucketBatches2 = accum.getDeque(DATA1_PHYSICAL_TABLE_PATH, tb2);
        Deque<WriteBatch> bucketBatches3 = accum.getDeque(DATA1_PHYSICAL_TABLE_PATH, tb3);
        return (bucketBatches1 == null ? 0 : bucketBatches1.size())
                + (bucketBatches2 == null ? 0 : bucketBatches2.size())
                + (bucketBatches3 == null ? 0 : bucketBatches3.size());
    }
}
