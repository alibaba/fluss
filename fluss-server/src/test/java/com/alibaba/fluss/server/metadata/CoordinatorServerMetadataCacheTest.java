/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.server.metadata;

import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.coordinator.CoordinatorContext;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.alibaba.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.server.zk.data.LeaderAndIsr.NO_LEADER;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CoordinatorServerMetadataCache}. */
public class CoordinatorServerMetadataCacheTest {
    private CoordinatorContext coordinatorContext;
    private CoordinatorServerMetadataCache serverMetadataCache;

    private ServerInfo coordinatorServer;
    private Set<ServerInfo> aliveTableServers;
    private final TablePath partitionedTablePath =
            TablePath.of("test_db_1", "test_partition_table_1");

    @BeforeEach
    public void setup() {
        coordinatorContext = new CoordinatorContext();
        serverMetadataCache = new CoordinatorServerMetadataCache(coordinatorContext);

        coordinatorServer =
                new ServerInfo(
                        0,
                        Endpoint.fromListenersString(
                                "CLIENT://localhost:99,INTERNAL://localhost:100"),
                        ServerType.COORDINATOR);

        aliveTableServers =
                new HashSet<>(
                        Arrays.asList(
                                new ServerInfo(
                                        0,
                                        Endpoint.fromListenersString(
                                                "CLIENT://localhost:101, INTERNAL://localhost:102"),
                                        ServerType.TABLET_SERVER),
                                new ServerInfo(
                                        1,
                                        Endpoint.fromListenersString("INTERNAL://localhost:103"),
                                        ServerType.TABLET_SERVER),
                                new ServerInfo(
                                        2,
                                        Endpoint.fromListenersString("INTERNAL://localhost:104"),
                                        ServerType.TABLET_SERVER)));
    }

    @Test
    void testCoordinatorServerMetadataCache() {
        long partitionTableId = 150002L;
        long partitionId1 = 15L;
        String partitionName1 = "p1";
        long partitionId2 = 16L;
        String partitionName2 = "p2";

        coordinatorContext.setCoordinatorServerInfo(coordinatorServer);
        aliveTableServers.forEach(
                aliveTableServer -> coordinatorContext.addLiveTabletServer(aliveTableServer));
        addTableToCoordinatorContext(
                DATA1_TABLE_ID,
                DATA1_TABLE_PATH,
                DATA1_TABLE_INFO,
                Arrays.asList(
                        new TableBucket(DATA1_TABLE_ID, 0), new TableBucket(DATA1_TABLE_ID, 1)),
                Arrays.asList(
                        new LeaderAndIsr(0, 0, Arrays.asList(0, 1, 2), 0, 0),
                        new LeaderAndIsr(-1, 0, Arrays.asList(0, 1, 2), 0, 0)));

        TableInfo partitionTableInfo =
                TableInfo.of(
                        partitionedTablePath,
                        partitionTableId,
                        0,
                        DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                        100L,
                        100L);
        addTableToCoordinatorContext(
                partitionTableId,
                partitionedTablePath,
                partitionTableInfo,
                Arrays.asList(
                        new TableBucket(partitionTableId, partitionId1, 0),
                        new TableBucket(partitionTableId, partitionId1, 1),
                        new TableBucket(partitionTableId, partitionId2, 0),
                        new TableBucket(partitionTableId, partitionId2, 1)),
                Arrays.asList(
                        new LeaderAndIsr(0, 0, Arrays.asList(0, 1, 2), 0, 0),
                        new LeaderAndIsr(NO_LEADER, 0, Arrays.asList(0, 1, 2), 0, 0),
                        new LeaderAndIsr(0, 0, Arrays.asList(0, 1, 2), 0, 0),
                        new LeaderAndIsr(NO_LEADER, 0, Arrays.asList(0, 1, 2), 0, 0)));
        coordinatorContext.putPartition(
                partitionId1, PhysicalTablePath.of(partitionedTablePath, partitionName1));
        coordinatorContext.putPartition(
                partitionId2, PhysicalTablePath.of(partitionedTablePath, partitionName2));

        assertThat(serverMetadataCache.getCoordinatorServer("CLIENT"))
                .isEqualTo(coordinatorServer.node("CLIENT"));
        assertThat(serverMetadataCache.getCoordinatorServer("INTERNAL"))
                .isEqualTo(coordinatorServer.node("INTERNAL"));
        assertThat(serverMetadataCache.isAliveTabletServer(0)).isTrue();
        assertThat(serverMetadataCache.getAllAliveTabletServers("CLIENT").size()).isEqualTo(1);
        assertThat(serverMetadataCache.getAllAliveTabletServers("INTERNAL").size()).isEqualTo(3);
        assertThat(serverMetadataCache.getAliveTabletServerIds())
                .containsExactlyInAnyOrder(0, 1, 2);

        assertThat(serverMetadataCache.getTablePath(DATA1_TABLE_ID).get())
                .isEqualTo(DATA1_TABLE_PATH);
        assertThat(serverMetadataCache.getTablePath(partitionTableId).get())
                .isEqualTo(TablePath.of("test_db_1", "test_partition_table_1"));

        assertThat(serverMetadataCache.getTableInfo(DATA1_TABLE_ID).get())
                .isEqualTo(DATA1_TABLE_INFO);
        assertThat(serverMetadataCache.getTableInfo(partitionTableId).get())
                .isEqualTo(
                        TableInfo.of(
                                partitionedTablePath,
                                partitionTableId,
                                0,
                                DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                                100L,
                                100L));

        assertTableMetadataEquals(
                DATA1_TABLE_ID,
                DATA1_TABLE_INFO,
                Arrays.asList(
                        new BucketMetadata(0, 0, 0, Arrays.asList(0, 1, 2)),
                        new BucketMetadata(1, NO_LEADER, 0, Arrays.asList(0, 1, 2))));

        assertPartitionMetadataEquals(
                partitionId1,
                partitionTableId,
                partitionId1,
                partitionName1,
                Arrays.asList(
                        new BucketMetadata(0, 0, 0, Arrays.asList(0, 1, 2)),
                        new BucketMetadata(1, NO_LEADER, 0, Arrays.asList(0, 1, 2))));
        assertPartitionMetadataEquals(
                partitionId2,
                partitionTableId,
                partitionId2,
                partitionName2,
                Arrays.asList(
                        new BucketMetadata(0, 0, 0, Arrays.asList(0, 1, 2)),
                        new BucketMetadata(1, NO_LEADER, 0, Arrays.asList(0, 1, 2))));

        // test partial update bucket info as setting NO_LEADER to 1 for bucketId = 1
        coordinatorContext.putBucketLeaderAndIsr(
                new TableBucket(DATA1_TABLE_ID, 1),
                new LeaderAndIsr(1, 0, Arrays.asList(0, 1, 2), 0, 0));
        coordinatorContext.putBucketLeaderAndIsr(
                new TableBucket(partitionTableId, partitionId1, 1),
                new LeaderAndIsr(1, 0, Arrays.asList(0, 1, 2), 0, 0));

        assertTableMetadataEquals(
                DATA1_TABLE_ID,
                DATA1_TABLE_INFO,
                Arrays.asList(
                        new BucketMetadata(0, 0, 0, Arrays.asList(0, 1, 2)),
                        new BucketMetadata(1, 1, 0, Arrays.asList(0, 1, 2))));

        assertPartitionMetadataEquals(
                partitionId1,
                partitionTableId,
                partitionId1,
                partitionName1,
                Arrays.asList(
                        new BucketMetadata(0, 0, 0, Arrays.asList(0, 1, 2)),
                        new BucketMetadata(1, 1, 0, Arrays.asList(0, 1, 2))));
        assertPartitionMetadataEquals(
                partitionId2,
                partitionTableId,
                partitionId2,
                partitionName2,
                Arrays.asList(
                        new BucketMetadata(0, 0, 0, Arrays.asList(0, 1, 2)),
                        new BucketMetadata(1, NO_LEADER, 0, Arrays.asList(0, 1, 2))));

        // test delete one table.
        coordinatorContext.removeTable(DATA1_TABLE_ID);
        assertThat(serverMetadataCache.getTablePath(DATA1_TABLE_ID)).isEmpty();
        assertThat(serverMetadataCache.getTableInfo(DATA1_TABLE_ID)).isEmpty();
        assertThat(serverMetadataCache.getTableMetadata(DATA1_TABLE_PATH)).isEmpty();

        // test delete one partition.
        coordinatorContext.removePartition(new TablePartition(partitionTableId, partitionId1));
        assertThat(
                        serverMetadataCache.getPartitionMetadata(
                                PhysicalTablePath.of(partitionedTablePath, partitionName1)))
                .isEmpty();
        assertPartitionMetadataEquals(
                partitionId2,
                partitionTableId,
                partitionId2,
                partitionName2,
                Arrays.asList(
                        new BucketMetadata(0, 0, 0, Arrays.asList(0, 1, 2)),
                        new BucketMetadata(1, NO_LEADER, 0, Arrays.asList(0, 1, 2))));
    }

    private void addTableToCoordinatorContext(
            long tableId,
            TablePath tablePath,
            TableInfo tableInfo,
            List<TableBucket> tableBucketList,
            List<LeaderAndIsr> leaderAndIsrList) {
        coordinatorContext.putTablePath(tableId, tablePath);
        coordinatorContext.putTableInfo(tableInfo);
        for (int i = 0; i < tableBucketList.size(); i++) {
            LeaderAndIsr leaderAndIsr = leaderAndIsrList.get(i);
            coordinatorContext.updateBucketReplicaAssignment(
                    tableBucketList.get(i), leaderAndIsr.isr());
            coordinatorContext.putBucketLeaderAndIsr(tableBucketList.get(i), leaderAndIsr);
        }
    }

    private void assertTableMetadataEquals(
            long tableId,
            TableInfo expectedTableInfo,
            List<BucketMetadata> expectedBucketMetadataList) {
        TablePath tablePath = serverMetadataCache.getTablePath(tableId).get();
        TableMetadata tableMetadata = serverMetadataCache.getTableMetadata(tablePath).get();
        assertThat(tableMetadata.getTableInfo()).isEqualTo(expectedTableInfo);
        assertThat(tableMetadata.getBucketMetadataList()).containsAll(expectedBucketMetadataList);
    }

    private void assertPartitionMetadataEquals(
            long partitionId,
            long expectedTableId,
            long expectedPartitionId,
            String expectedPartitionName,
            List<BucketMetadata> expectedBucketMetadataList) {
        String actualPartitionName =
                serverMetadataCache.getPhysicalTablePath(partitionId).get().getPartitionName();
        assertThat(actualPartitionName).isEqualTo(expectedPartitionName);
        PartitionMetadata partitionMetadata =
                serverMetadataCache
                        .getPartitionMetadata(
                                PhysicalTablePath.of(partitionedTablePath, actualPartitionName))
                        .get();
        assertThat(partitionMetadata.getTableId()).isEqualTo(expectedTableId);
        assertThat(partitionMetadata.getPartitionId()).isEqualTo(expectedPartitionId);
        assertThat(partitionMetadata.getPartitionName()).isEqualTo(actualPartitionName);
        assertThat(partitionMetadata.getBucketMetadataList())
                .containsAll(expectedBucketMetadataList);
    }
}
