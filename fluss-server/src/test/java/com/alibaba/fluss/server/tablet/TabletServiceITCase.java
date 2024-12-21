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

package com.alibaba.fluss.server.tablet;

import com.alibaba.fluss.exception.InvalidRequiredAcksException;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.DefaultKvRecordBatch;
import com.alibaba.fluss.record.DefaultValueRecordBatch;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.row.encode.ValueEncoder;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.FetchLogResponse;
import com.alibaba.fluss.rpc.messages.InitWriterRequest;
import com.alibaba.fluss.rpc.messages.InitWriterResponse;
import com.alibaba.fluss.rpc.messages.ListOffsetsResponse;
import com.alibaba.fluss.rpc.messages.PbFetchLogRespForBucket;
import com.alibaba.fluss.rpc.messages.PbFetchLogRespForTable;
import com.alibaba.fluss.rpc.messages.PbListOffsetsRespForBucket;
import com.alibaba.fluss.rpc.messages.PbLookupRespForBucket;
import com.alibaba.fluss.rpc.messages.PbPutKvRespForBucket;
import com.alibaba.fluss.rpc.messages.PutKvResponse;
import com.alibaba.fluss.rpc.protocol.Errors;
import com.alibaba.fluss.server.log.ListOffsetsParam;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.record.TestData.ANOTHER_DATA1;
import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_KEY_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static com.alibaba.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE;
import static com.alibaba.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static com.alibaba.fluss.server.testutils.KvTestUtils.assertLookupResponse;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.assertFetchLogResponse;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.assertLimitScanResponse;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.assertProduceLogResponse;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newFetchLogRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newLimitScanRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newListOffsetsRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;
import static com.alibaba.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for {@link TabletService}. */
public class TabletServiceITCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    @Test
    void testProduceLog() throws Exception {
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        DATA1_TABLE_PATH,
                        DATA1_TABLE_INFO.getTableDescriptor());
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        // 1. send first batch.
        assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                newProduceLogRequest(
                                        tableId, 0, 1, genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                0,
                0L);

        // 2. send second batch.
        assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                newProduceLogRequest(
                                        tableId, 0, 1, genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                0,
                10L);

        // 3. test produce with error acks to check error produce record.
        assertThatThrownBy(
                        () ->
                                leaderGateWay
                                        .produceLog(
                                                newProduceLogRequest(
                                                        tableId,
                                                        0,
                                                        100,
                                                        genMemoryLogRecordsByObject(DATA1)))
                                        .get())
                .cause()
                .isInstanceOf(InvalidRequiredAcksException.class)
                .hasMessageContaining("Invalid required acks");
    }

    @Test
    void testFetchLog() throws Exception {
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        DATA1_TABLE_PATH,
                        DATA1_TABLE_INFO.getTableDescriptor());
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // produce one batch to this bucket.
        assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                newProduceLogRequest(
                                        tableId, 0, 1, genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                0,
                0L);

        // fetch from this bucket from offset 0, return data1.
        assertFetchLogResponse(
                leaderGateWay.fetchLog(newFetchLogRequest(-1, tableId, 0, 0L)).get(),
                tableId,
                0,
                10L,
                DATA1);

        // fetch from this bucket from offset 3, return data1.
        assertFetchLogResponse(
                leaderGateWay.fetchLog(newFetchLogRequest(-1, tableId, 0, 3L)).get(),
                tableId,
                0,
                10L,
                DATA1);

        // append new batch.
        assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                newProduceLogRequest(
                                        tableId, 0, 1, genMemoryLogRecordsByObject(ANOTHER_DATA1)))
                        .get(),
                0,
                10L);

        // fetch this bucket from offset 10, return data2.
        assertFetchLogResponse(
                leaderGateWay.fetchLog(newFetchLogRequest(-1, tableId, 0, 10L)).get(),
                tableId,
                0,
                20L,
                ANOTHER_DATA1);

        // fetch this bucket from offset 100, return error code.
        assertFetchLogResponse(
                leaderGateWay.fetchLog(newFetchLogRequest(-1, tableId, 0, 100L)).get(),
                tableId,
                0,
                Errors.LOG_OFFSET_OUT_OF_RANGE_EXCEPTION.code(),
                "Received request for offset");

        // fetch only first field
        List<Object[]> totalData = new ArrayList<>(DATA1);
        totalData.addAll(ANOTHER_DATA1);
        List<Object[]> expected1 = new ArrayList<>();
        for (int i = 0; i < ANOTHER_DATA1.size(); i++) {
            expected1.add(new Object[] {totalData.get(i)[0]});
        }
        assertFetchLogResponse(
                leaderGateWay
                        .fetchLog(newFetchLogRequest(-1, tableId, 0, 10L, new int[] {0}))
                        .get(),
                DATA1_ROW_TYPE.project(new int[] {0}),
                tableId,
                0,
                20L,
                expected1);

        // fetch only second field, results contains from offset 10 ~ 20, even fetchOffset=15L
        List<Object[]> expected2 = new ArrayList<>();
        for (int i = 10; i < totalData.size(); i++) {
            expected2.add(new Object[] {totalData.get(i)[1]});
        }
        assertFetchLogResponse(
                leaderGateWay
                        .fetchLog(newFetchLogRequest(-1, tableId, 0, 15L, new int[] {1}))
                        .get(),
                DATA1_ROW_TYPE.project(new int[] {1}),
                tableId,
                0,
                20L,
                expected2);

        assertFetchLogResponse(
                leaderGateWay
                        .fetchLog(newFetchLogRequest(-1, tableId, 0, 10L, new int[] {2, 3}))
                        .get(),
                tableId,
                0,
                Errors.INVALID_COLUMN_PROJECTION.code(),
                "Projected fields [2, 3] is out of bound for schema with 2 fields.");
    }

    @Test
    void testFetchLogWithMinFetchSizeAndTimeout() throws Exception {
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        DATA1_TABLE_PATH,
                        DATA1_TABLE_INFO.getTableDescriptor());
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // first send an empty fetch request, without min fetch size, the request will return
        // immediately.
        FetchLogResponse fetchLogResponse =
                leaderGateWay.fetchLog(newFetchLogRequest(-1, tableId, 0, 0L)).get();
        assertThat(fetchLogResponse.getTablesRespsCount()).isEqualTo(1);
        PbFetchLogRespForTable fetchLogRespForTable = fetchLogResponse.getTablesRespsList().get(0);
        assertThat(fetchLogRespForTable.getTableId()).isEqualTo(tableId);
        assertThat(fetchLogRespForTable.getBucketsRespsCount()).isEqualTo(1);
        PbFetchLogRespForBucket protoFetchedBucket =
                fetchLogRespForTable.getBucketsRespsList().get(0);
        assertThat(protoFetchedBucket.getHighWatermark()).isEqualTo(0L);
        assertThat(protoFetchedBucket.getRecordsSize()).isEqualTo(0);

        // second send a fetch request with minFetchSize and small maxFetchWaitMs, the request will
        // also return immediately.
        fetchLogResponse =
                leaderGateWay
                        .fetchLog(
                                newFetchLogRequest(
                                        -1, tableId, 0, 0L, null, 1, Integer.MAX_VALUE, 100))
                        .get();
        assertThat(fetchLogResponse.getTablesRespsCount()).isEqualTo(1);
        fetchLogRespForTable = fetchLogResponse.getTablesRespsList().get(0);
        assertThat(fetchLogRespForTable.getTableId()).isEqualTo(tableId);
        assertThat(fetchLogRespForTable.getBucketsRespsCount()).isEqualTo(1);
        protoFetchedBucket = fetchLogRespForTable.getBucketsRespsList().get(0);
        assertThat(protoFetchedBucket.getHighWatermark()).isEqualTo(0L);
        assertThat(protoFetchedBucket.getRecordsSize()).isEqualTo(0);

        // third send a fetch request with minFetchSize and much bigger maxFetchWaitMs, the request
        // will return after we send a produce log request to this bucket.
        leaderGateWay
                .fetchLog(
                        newFetchLogRequest(
                                -1,
                                tableId,
                                0,
                                0L,
                                null,
                                1,
                                Integer.MAX_VALUE,
                                (int) Duration.ofMinutes(5).toMillis()))
                .whenComplete(
                        (res, throwable) -> assertFetchLogResponse(res, tableId, 0, 10L, DATA1));

        // send a produce log request to trigger delay fetch log finish.
        assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                newProduceLogRequest(
                                        tableId, 0, 1, genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                0,
                0L);

        // return immediately.
        assertFetchLogResponse(
                leaderGateWay
                        .fetchLog(
                                newFetchLogRequest(
                                        -1,
                                        tableId,
                                        0,
                                        0L,
                                        null,
                                        1,
                                        Integer.MAX_VALUE,
                                        (int) Duration.ofMinutes(5).toMillis()))
                        .get(),
                tableId,
                0,
                10L,
                DATA1);
    }

    @Test
    void testInvalidFetchLog() throws Exception {
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        TablePath.of("test_db_1", "test_indexed_table_1"),
                        TableDescriptor.builder()
                                .schema(DATA1_SCHEMA)
                                .logFormat(LogFormat.INDEXED)
                                .distributedBy(3)
                                .build());
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        assertFetchLogResponse(
                leaderGateWay.fetchLog(newFetchLogRequest(-1, tableId, 0, 0L, new int[] {1})).get(),
                tableId,
                0,
                Errors.INVALID_COLUMN_PROJECTION.code(),
                "Column projection is only supported for ARROW format, "
                        + "but the table test_db_1.test_indexed_table_1 is INDEXED format.");
    }

    @Test
    void testPutKv() throws Exception {
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        DATA1_TABLE_PATH_PK,
                        DATA1_TABLE_INFO_PK.getTableDescriptor());
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        // 1. send one batch kv.
        assertPutKvResponse(
                leaderGateWay
                        .putKv(
                                newPutKvRequest(
                                        tableId, 0, 1, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)))
                        .get());

        // 2. test put with error acks to check error produce record.
        assertThatThrownBy(
                        () ->
                                leaderGateWay
                                        .putKv(
                                                newPutKvRequest(
                                                        tableId,
                                                        0,
                                                        100,
                                                        genKvRecordBatch(
                                                                DATA_1_WITH_KEY_AND_VALUE)))
                                        .get())
                .cause()
                .isInstanceOf(InvalidRequiredAcksException.class)
                .hasMessageContaining("Invalid required acks");
    }

    @Test
    void testGetKey() throws Exception {
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        DATA1_TABLE_PATH_PK,
                        DATA1_TABLE_INFO_PK.getTableDescriptor());
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // first get key without in table, key = 1.
        Object[] key1 = DATA_1_WITH_KEY_AND_VALUE.get(0).f0;
        KeyEncoder keyEncoder = new KeyEncoder(DATA1_ROW_TYPE, new int[] {0});
        byte[] key1Bytes = keyEncoder.encode(row(DATA1_KEY_TYPE, key1));
        assertLookupResponse(
                leaderGateWay.lookup(newLookupRequest(tableId, 0, key1Bytes)).get(), null);

        // send one batch kv.
        assertPutKvResponse(
                leaderGateWay
                        .putKv(
                                newPutKvRequest(
                                        tableId, 0, 1, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)))
                        .get());

        // second get key in table, key = 1, value = 1, "a1".
        Object[] value1 = DATA_1_WITH_KEY_AND_VALUE.get(3).f1;
        byte[] value1Bytes =
                ValueEncoder.encodeValue(DEFAULT_SCHEMA_ID, compactedRow(DATA1_ROW_TYPE, value1));
        assertLookupResponse(
                leaderGateWay.lookup(newLookupRequest(tableId, 0, key1Bytes)).get(), value1Bytes);

        // key = 3 is deleted, need return null.
        Object[] key3 = DATA_1_WITH_KEY_AND_VALUE.get(2).f0;
        byte[] key3Bytes = keyEncoder.encode(row(DATA1_KEY_TYPE, key3));
        assertLookupResponse(
                leaderGateWay.lookup(newLookupRequest(tableId, 0, key3Bytes)).get(), null);

        // Lookup from an unknown table-bucket.
        PbLookupRespForBucket pbLookupRespForBucket =
                leaderGateWay
                        .lookup(newLookupRequest(10005L, 6, key3Bytes))
                        .get()
                        .getBucketsRespAt(0);

        verifyError(
                pbLookupRespForBucket,
                Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION,
                "Unknown table or bucket: TableBucket{tableId=10005, bucket=6}");

        // Get key from a non-pk table.
        long logTableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        DATA1_TABLE_PATH,
                        DATA1_TABLE_INFO.getTableDescriptor());
        TableBucket logTableBucket = new TableBucket(logTableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(logTableBucket);

        int logLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(logTableBucket);
        TabletServerGateway logLeaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(logLeader);
        pbLookupRespForBucket =
                logLeaderGateWay
                        .lookup(newLookupRequest(logTableId, 0, key3Bytes))
                        .get()
                        .getBucketsRespAt(0);
        verifyError(
                pbLookupRespForBucket,
                Errors.NON_PRIMARY_KEY_TABLE_EXCEPTION,
                "the primary key table not exists for TableBucket");
    }

    @Test
    void testLimitScanPrimaryKeyTable() throws Exception {
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        DATA1_TABLE_PATH_PK,
                        DATA1_TABLE_INFO_PK.getTableDescriptor());
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        DefaultValueRecordBatch.Builder builder = DefaultValueRecordBatch.builder();

        // first limit scan from empty table.
        assertLimitScanResponse(
                leaderGateWay.limitScan(newLimitScanRequest(tableId, 0, 1)).get(), builder.build());

        // send one batch kv.
        DefaultKvRecordBatch kvRecordBatch =
                (DefaultKvRecordBatch) genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE);
        assertPutKvResponse(
                leaderGateWay.putKv(newPutKvRequest(tableId, 0, 1, kvRecordBatch)).get());
        builder.append(DEFAULT_SCHEMA_ID, compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a1"}));
        // second limit scan from table
        assertLimitScanResponse(
                leaderGateWay.limitScan(newLimitScanRequest(tableId, 0, 1)).get(), builder.build());
        builder.append(DEFAULT_SCHEMA_ID, compactedRow(DATA1_ROW_TYPE, new Object[] {2, "b1"}));
        assertLimitScanResponse(
                leaderGateWay.limitScan(newLimitScanRequest(tableId, 0, 3)).get(), builder.build());
    }

    @Test
    void testLimitScanLogTable() throws Exception {
        long logTableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        DATA1_TABLE_PATH,
                        DATA1_TABLE_INFO.getTableDescriptor());
        TableBucket logTableBucket = new TableBucket(logTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(logTableBucket);
        int logLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(logTableBucket);
        TabletServerGateway logLeaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(logLeader);
        // send first batch.
        assertProduceLogResponse(
                logLeaderGateWay
                        .produceLog(
                                newProduceLogRequest(
                                        logTableId, 0, 1, genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                0,
                0L);

        // append new batch.
        assertProduceLogResponse(
                logLeaderGateWay
                        .produceLog(
                                newProduceLogRequest(
                                        logTableId,
                                        0,
                                        1,
                                        genMemoryLogRecordsByObject(ANOTHER_DATA1)))
                        .get(),
                0,
                10L);

        // fetch only second field, results contains from offset 10 ~ 20, even fetchOffset=15L
        List<Object[]> expected2 = new ArrayList<>(ANOTHER_DATA1);

        // limit log table scan will get the latest limit number of data.
        assertLimitScanResponse(
                logLeaderGateWay.limitScan(newLimitScanRequest(logTableId, 0, 10)).get(),
                DATA1_ROW_TYPE,
                expected2);
    }

    @Test
    void testListOffsets() throws Exception {
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        DATA1_TABLE_PATH,
                        DATA1_TABLE_INFO.getTableDescriptor());
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // produce one batch to this bucket.
        assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                newProduceLogRequest(
                                        tableId, 0, 1, genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                0,
                0L);

        // listOffset from client.
        assertListOffsetsResponse(
                leaderGateWay
                        .listOffsets(
                                newListOffsetsRequest(
                                        -1, ListOffsetsParam.LATEST_OFFSET_TYPE, tableId, 0))
                        .get(),
                10L,
                Errors.NONE.code(),
                null);

        // listOffset from tablet server where follower locate in.
        assertListOffsetsResponse(
                leaderGateWay
                        .listOffsets(
                                newListOffsetsRequest(
                                        1, ListOffsetsParam.LATEST_OFFSET_TYPE, tableId, 0))
                        .get(),
                10L,
                Errors.NONE.code(),
                null);

        // list an unknown table id.
        assertListOffsetsResponse(
                leaderGateWay
                        .listOffsets(
                                newListOffsetsRequest(
                                        1, ListOffsetsParam.LATEST_OFFSET_TYPE, 10005L, 6))
                        .get(),
                null,
                Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION.code(),
                "Unknown table or bucket: TableBucket{tableId=10005, bucket=6}");
    }

    @Test
    void testInitWriterId() throws Exception {
        TabletServerGateway tabletServerGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(0);
        for (int i = 0; i < 100; i++) {
            InitWriterResponse response =
                    tabletServerGateway.initWriter(new InitWriterRequest()).get();
            assertThat(response.getWriterId()).isEqualTo(i);
        }

        FLUSS_CLUSTER_EXTENSION.stopCoordinatorServer();
        // start again.
        FLUSS_CLUSTER_EXTENSION.startCoordinatorServer();

        for (int i = 100; i < 200; i++) {
            InitWriterResponse response =
                    tabletServerGateway.initWriter(new InitWriterRequest()).get();
            assertThat(response.getWriterId()).isEqualTo(i);
        }
    }

    private static void assertPutKvResponse(PutKvResponse putKvResponse) {
        assertThat(putKvResponse.getBucketsRespsCount()).isEqualTo(1);
        PbPutKvRespForBucket putKvRespForBucket = putKvResponse.getBucketsRespsList().get(0);
        assertThat(putKvRespForBucket.getBucketId()).isEqualTo(0);
    }

    private static void assertListOffsetsResponse(
            ListOffsetsResponse listOffsetsResponse,
            @Nullable Long offset,
            Integer errorCode,
            @Nullable String errorMessage) {
        assertThat(listOffsetsResponse.getBucketsRespsCount()).isEqualTo(1);
        PbListOffsetsRespForBucket respForBucket = listOffsetsResponse.getBucketsRespsList().get(0);
        if (respForBucket.hasErrorCode()) {
            assertThat(respForBucket.getErrorCode()).isEqualTo(errorCode);
            assertThat(respForBucket.getErrorMessage()).contains(errorMessage);
        } else {
            assertThat(respForBucket.getOffset()).isEqualTo(offset);
        }
    }

    private static void verifyError(
            PbLookupRespForBucket lookupRespForBucket,
            Errors expectedError,
            String expectErrMessage) {
        assertThat(lookupRespForBucket.hasErrorCode()).isTrue();
        assertThat(lookupRespForBucket.getErrorCode()).isEqualTo(expectedError.code());
        assertThat(lookupRespForBucket.getErrorMessage()).contains(expectErrMessage);
    }
}
