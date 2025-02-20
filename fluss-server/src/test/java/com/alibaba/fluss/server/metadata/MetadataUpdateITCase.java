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

package com.alibaba.fluss.server.metadata;

import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.gateway.AdminGateway;
import com.alibaba.fluss.rpc.gateway.AdminReadOnlyGateway;
import com.alibaba.fluss.rpc.messages.DropTableRequest;
import com.alibaba.fluss.rpc.messages.MetadataRequest;
import com.alibaba.fluss.rpc.messages.MetadataResponse;
import com.alibaba.fluss.rpc.messages.PbBucketMetadata;
import com.alibaba.fluss.rpc.messages.PbPartitionMetadata;
import com.alibaba.fluss.rpc.messages.PbTableMetadata;
import com.alibaba.fluss.server.tablet.TabletServer;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newCreateTableRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newGetTableInfoRequest;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitUtil;
import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for metadata update. */
class MetadataUpdateITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    @Test
    void testMetadataUpdate() throws Exception {
        // get metadata and check it
        FLUSS_CLUSTER_EXTENSION.waitUtilAllGatewayHasSameMetadata();

        // first, create two tables
        TablePath nonPartitionedTablePath = TablePath.of("fluss", "non_partitioned_table");
        long nonPartitionedTableId = createTable(nonPartitionedTablePath);
        TablePath partitionedTablePath = TablePath.of("fluss", "partitioned_table");
        long partitionedTableId = createPartitionedTable(partitionedTablePath);
        Map<String, Long> partitionIdByName =
                FLUSS_CLUSTER_EXTENSION.waitUtilPartitionAllReady(partitionedTablePath);
        Set<PhysicalTablePath> physicalTablePaths = new HashSet<>();
        for (String partition : partitionIdByName.keySet()) {
            physicalTablePaths.add(PhysicalTablePath.of(partitionedTablePath, partition));
        }

        // verify the cache metadata
        retry(
                Duration.ofMinutes(1),
                () -> verifyBucketLeader(nonPartitionedTablePath, physicalTablePaths));

        // now, start one tablet server and check it
        FLUSS_CLUSTER_EXTENSION.startTabletServer(3);
        FLUSS_CLUSTER_EXTENSION.waitUtilAllGatewayHasSameMetadata();
        // verify the new tablet server should contain all metadata cache
        retry(
                Duration.ofMinutes(1),
                () ->
                        verifyBucketLeader(
                                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(3),
                                nonPartitionedTablePath,
                                physicalTablePaths));

        // now, kill one tablet server and check it
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(1);
        FLUSS_CLUSTER_EXTENSION.waitUtilAllGatewayHasSameMetadata();
        retry(
                Duration.ofMinutes(1),
                () -> verifyBucketLeader(nonPartitionedTablePath, physicalTablePaths, 1));

        // let's stop the coordinator server
        FLUSS_CLUSTER_EXTENSION.stopCoordinatorServer();
        // then kill one tablet server again
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(2);
        // let's start the coordinator server again;
        FLUSS_CLUSTER_EXTENSION.startCoordinatorServer();
        // check the metadata again
        FLUSS_CLUSTER_EXTENSION.waitUtilAllGatewayHasSameMetadata();
        retry(
                Duration.ofMinutes(1),
                () -> verifyBucketLeader(nonPartitionedTablePath, physicalTablePaths, 2));

        // now, we drop all tables
        dropTable(nonPartitionedTablePath);
        dropTable(partitionedTablePath);

        // collect all buckets of the tables
        Set<TableBucket> allBuckets = new HashSet<>();
        for (int bucket = 0; bucket < 3; bucket++) {
            allBuckets.add(new TableBucket(nonPartitionedTableId, bucket));
            for (long partitionId : partitionIdByName.values()) {
                allBuckets.add(new TableBucket(partitionedTableId, partitionId, bucket));
            }
        }
        // verify bucket leader info should be removed after table dropped
        retry(Duration.ofMinutes(1), () -> verifyBucketLeaderEmpty(allBuckets));
    }

    private long createTable(TablePath tablePath) throws Exception {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("c1", DataTypes.INT()).build())
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 3)
                        .build();
        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();
        return adminGateway.getTableInfo(newGetTableInfoRequest(tablePath)).get().getTableId();
    }

    private void dropTable(TablePath tablePath) throws Exception {
        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        DropTableRequest dropTableRequest = new DropTableRequest().setIgnoreIfNotExists(false);
        dropTableRequest
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        adminGateway.dropTable(dropTableRequest).get();
    }

    private long createPartitionedTable(TablePath tablePath) throws Exception {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .build())
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 3)
                        .partitionedBy("c2")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.DAY)
                        .build();
        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();
        return adminGateway.getTableInfo(newGetTableInfoRequest(tablePath)).get().getTableId();
    }

    private void verifyBucketLeader(
            TablePath tablePath, Collection<PhysicalTablePath> partitionPaths) throws Exception {
        for (AdminReadOnlyGateway gateway : FLUSS_CLUSTER_EXTENSION.collectAllRpcGateways()) {
            verifyBucketLeader(gateway, tablePath, partitionPaths);
        }
    }

    private void verifyBucketLeader(
            TablePath tablePath, Collection<PhysicalTablePath> partitionPaths, int blackLeader)
            throws Exception {
        for (AdminReadOnlyGateway gateway : FLUSS_CLUSTER_EXTENSION.collectAllRpcGateways()) {
            verifyBucketLeader(gateway, tablePath, partitionPaths, blackLeader);
        }
    }

    private void verifyBucketLeader(
            AdminReadOnlyGateway gateway,
            TablePath tablePath,
            Collection<PhysicalTablePath> partitionPaths)
            throws Exception {
        verifyBucketLeader(gateway, tablePath, partitionPaths, null);
    }

    private void verifyBucketLeader(
            AdminReadOnlyGateway gateway,
            TablePath tablePath,
            Collection<PhysicalTablePath> partitionPaths,
            @Nullable Integer blackLeader)
            throws Exception {
        MetadataRequest metadataRequest = new MetadataRequest();
        // for table
        metadataRequest
                .addTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        for (PhysicalTablePath partitionPath : partitionPaths) {
            metadataRequest
                    .addPartitionsPath()
                    .setDatabaseName(partitionPath.getDatabaseName())
                    .setTableName(partitionPath.getTableName())
                    .setPartitionName(partitionPath.getPartitionName());
        }

        MetadataResponse metadataResponse = gateway.metadata(metadataRequest).get();

        // verify tables
        for (PbTableMetadata tableMetadata : metadataResponse.getTableMetadatasList()) {
            long tableId = tableMetadata.getTableId();
            for (PbBucketMetadata bucketMetadata : tableMetadata.getBucketMetadatasList()) {
                int bucketId = bucketMetadata.getBucketId();
                TableBucket tableBucket = new TableBucket(tableId, bucketId);
                int expectedLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tableBucket);
                if (blackLeader != null) {
                    // need to wait util the leader is not the black leader
                    waitUtil(
                            () ->
                                    FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tableBucket)
                                            != blackLeader,
                            Duration.ofMinutes(1),
                            "Fail to wait the leader to be different from the leader "
                                    + blackLeader);
                    expectedLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tableBucket);
                }
                assertThat(bucketMetadata.hasLeaderId()).isTrue();
                assertThat(bucketMetadata.getLeaderId()).isEqualTo(expectedLeader);
            }
        }

        for (PbPartitionMetadata partitionMetadata : metadataResponse.getPartitionMetadatasList()) {
            long tableId = partitionMetadata.getTableId();
            long partitionId = partitionMetadata.getPartitionId();
            for (PbBucketMetadata bucketMetadata : partitionMetadata.getBucketMetadatasList()) {
                int bucketId = bucketMetadata.getBucketId();
                TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
                int expectedLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tableBucket);
                if (blackLeader != null) {
                    // need to wait util the leader is not the black leader
                    waitUtil(
                            () ->
                                    FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tableBucket)
                                            != blackLeader,
                            Duration.ofMinutes(1),
                            "Fail to wait the leader to be different from the leader "
                                    + blackLeader);
                    expectedLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tableBucket);
                }
                assertThat(bucketMetadata.hasLeaderId()).isTrue();
                assertThat(bucketMetadata.getLeaderId()).isEqualTo(expectedLeader);
            }
        }
    }

    private void verifyBucketLeaderEmpty(Set<TableBucket> tableBuckets) {
        // verify cache in coordinator server
        ServerMetadataCache coordinatorServerMetaCache =
                FLUSS_CLUSTER_EXTENSION.getCoordinatorServer().getServerMetadataCache();
        for (TableBucket tableBucket : tableBuckets) {
            assertThat(coordinatorServerMetaCache.getLeader(tableBucket)).isNull();
        }

        // verify cache in tablet server
        for (TabletServer tabletServer : FLUSS_CLUSTER_EXTENSION.getTabletServers()) {
            ServerMetadataCache tabletServerMetaCache = tabletServer.getServerMetadataCache();
            for (TableBucket tableBucket : tableBuckets) {
                assertThat(tabletServerMetaCache.getLeader(tableBucket)).isNull();
            }
        }
    }
}
