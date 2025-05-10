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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.utils.timer.DefaultTimer;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.utils.clock.ManualClock;
import com.alibaba.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.alibaba.fluss.server.coordinator.LakeTableTieringManager.TIERING_SERVICE_TIMEOUT_MS;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LakeTableTieringManager}. */
class LakeTableTieringManagerTest {

    private LakeTableTieringManager tableTieringManager;
    private ManualClock manualClock;

    @BeforeEach
    void beforeEach() {
        manualClock = new ManualClock();
        tableTieringManager = createLakeTableTieringManager();
    }

    private LakeTableTieringManager createLakeTableTieringManager() {
        return new LakeTableTieringManager(
                new DefaultTimer("delay lake tiering", 1_000, 20, manualClock), manualClock);
    }

    @Test
    void testInitLakeTableTieringManagerWithTables() {
        long tableId1 = 1L;
        TablePath tablePath1 = TablePath.of("db", "table");
        TableInfo tableInfo1 = createTableInfo(tableId1, tablePath1, Integer.MAX_VALUE);
        List<TableInfo> lakeTables = Collections.singletonList(tableInfo1);
        tableTieringManager.initWithLakeTables(lakeTables);

        // advance time to trigger the table tiering
        manualClock.advanceTime(Duration.ofMinutes(10));
        // retry, should be able to get the table to be tiered
        retry(Duration.ofSeconds(10), () -> assertRequestTable(tableId1, tablePath1));

        long tableId2 = 2L;
        TablePath tablePath2 = TablePath.of("db", "table2");
        TableInfo tableInfo2 = createTableInfo(tableId2, tablePath2, Integer.MAX_VALUE);
        lakeTables = Arrays.asList(tableInfo1, tableInfo2);
        tableTieringManager = createLakeTableTieringManager();
        tableTieringManager.initWithLakeTables(lakeTables);

        // mock tiering service send heartbeat to tiering manager that table1 is tiering
        tableTieringManager.renewTieringHeartbeat(tableId1);
        // advance time to trigger the table tiering
        manualClock.advanceTime(Duration.ofMinutes(10));
        // retry, should only get the table2 to be tiered
        retry(Duration.ofSeconds(10), () -> assertRequestTable(tableId2, tablePath2));
    }

    @Test
    void testAddNewLakeTable() {
        long tableId1 = 1L;
        TablePath tablePath1 = TablePath.of("db", "table");
        TableInfo tableInfo1 =
                createTableInfo(tableId1, tablePath1, Duration.ofSeconds(10).toMillis());
        tableTieringManager.addNewLakeTable(tableInfo1);

        // advance time to trigger the table tiering
        manualClock.advanceTime(Duration.ofSeconds(10));
        retry(Duration.ofSeconds(10), () -> assertRequestTable(tableId1, tablePath1));
    }

    @Test
    void testRemoveLakeTable() {
        long tableId1 = 1L;
        TablePath tablePath1 = TablePath.of("db", "table");
        TableInfo tableInfo1 =
                createTableInfo(tableId1, tablePath1, Duration.ofSeconds(10).toMillis());
        tableTieringManager.addNewLakeTable(tableInfo1);

        long tableId2 = 2L;
        TablePath tablePath2 = TablePath.of("db", "table2");
        TableInfo tableInfo2 =
                createTableInfo(tableId2, tablePath2, Duration.ofSeconds(10).toMillis());
        tableTieringManager.addNewLakeTable(tableInfo2);

        // remove the tableId1
        tableTieringManager.removeLakeTable(tableId1);

        // advance time to trigger the table tiering
        manualClock.advanceTime(Duration.ofSeconds(10));
        // shouldn't get tableId1, should only get tableId2
        retry(Duration.ofSeconds(10), () -> assertRequestTable(tableId2, tablePath2));
    }

    @Test
    void testFinishTableTieringReTriggerSchedule() {
        long tableId1 = 1L;
        TablePath tablePath1 = TablePath.of("db", "table");
        TableInfo tableInfo1 =
                createTableInfo(tableId1, tablePath1, Duration.ofSeconds(10).toMillis());
        tableTieringManager.addNewLakeTable(tableInfo1);

        manualClock.advanceTime(Duration.ofSeconds(10));
        // check requested table
        retry(Duration.ofSeconds(10), () -> assertRequestTable(tableId1, tablePath1));

        // request table should return null
        assertThat(tableTieringManager.requestTable()).isNull();

        // mock lake tiering finish one-round tiering
        tableTieringManager.finishTableTiering(tableId1);
        // not advance time, request table should return null
        assertThat(tableTieringManager.requestTable()).isNull();

        // now, advance time to trigger the table tiering
        manualClock.advanceTime(Duration.ofSeconds(10));
        retry(Duration.ofSeconds(10), () -> assertRequestTable(tableId1, tablePath1));
    }

    @Test
    void testTieringServiceTimeOutReTriggerPending() {
        long tableId1 = 1L;
        TablePath tablePath1 = TablePath.of("db", "table1");
        TableInfo tableInfo1 =
                createTableInfo(tableId1, tablePath1, Duration.ofSeconds(10).toMillis());
        tableTieringManager.addNewLakeTable(tableInfo1);
        long tableId2 = 2L;
        TablePath tablePath2 = TablePath.of("db", "table2");
        TableInfo tableInfo2 =
                createTableInfo(tableId2, tablePath2, Duration.ofSeconds(10).toMillis());
        tableTieringManager.addNewLakeTable(tableInfo2);

        manualClock.advanceTime(Duration.ofSeconds(10));
        // check requested table
        retry(Duration.ofSeconds(10), () -> assertRequestTable(tableId1, tablePath1));
        retry(Duration.ofSeconds(10), () -> assertRequestTable(tableId2, tablePath2));

        // advance time and mock tiering service heartbeat
        manualClock.advanceTime(Duration.ofMillis(TIERING_SERVICE_TIMEOUT_MS - 1));
        // tableid1 renew the tiering heartbeat, so that it won't be
        // re-pending after heartbeat timeout
        tableTieringManager.renewTieringHeartbeat(tableId1);
        // should only get table2
        manualClock.advanceTime(Duration.ofSeconds(10));
        retry(Duration.ofSeconds(10), () -> assertRequestTable(tableId2, tablePath2));

        // advance a large time to mock tiering service heartbeat timeout
        // and check the request table, the table1 should be re-scheduled
        manualClock.advanceTime(Duration.ofMinutes(5));
        retry(Duration.ofMinutes(1), () -> assertRequestTable(tableId1, tablePath1));
    }

    private TableInfo createTableInfo(long tableId, TablePath tablePath, long tieringIntervalMs) {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("c1", DataTypes.INT()).build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_DATALAKE_TIERING_INTERVAL,
                                Duration.ofMillis(tieringIntervalMs))
                        .distributedBy(1)
                        .build();

        return TableInfo.of(
                tablePath,
                tableId,
                1,
                tableDescriptor,
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }

    private void assertRequestTable(long tableId, TablePath tablePath) {
        Tuple2<Long, TablePath> tableIdAndPath = tableTieringManager.requestTable();
        assertThat(tableIdAndPath).isNotNull();
        assertThat(tableIdAndPath.f0).isEqualTo(tableId);
        assertThat(tableIdAndPath.f1).isEqualTo(tablePath);
    }
}
