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

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.coordinator.CoordinatorContext;
import com.alibaba.fluss.server.coordinator.CoordinatorServer;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.alibaba.fluss.server.zk.data.LeaderAndIsr.NO_LEADER;

/** The implement of {@link ServerMetadataCache} for {@link CoordinatorServer}. */
public class CoordinatorServerMetadataCache implements ServerMetadataCache {

    private final CoordinatorContext coordinatorContext;

    public CoordinatorServerMetadataCache(CoordinatorContext coordinatorContext) {
        this.coordinatorContext = coordinatorContext;
    }

    @Override
    public boolean isAliveTabletServer(int serverId) {
        Map<Integer, ServerInfo> aliveTabletServer = coordinatorContext.getLiveTabletServers();
        return aliveTabletServer.containsKey(serverId);
    }

    @Override
    public Optional<ServerNode> getTabletServer(int serverId, String listenerName) {
        Map<Integer, ServerInfo> aliveTabletServer = coordinatorContext.getLiveTabletServers();
        return aliveTabletServer.containsKey(serverId)
                ? Optional.ofNullable(aliveTabletServer.get(serverId).node(listenerName))
                : Optional.empty();
    }

    @Override
    public Map<Integer, ServerNode> getAllAliveTabletServers(String listenerName) {
        Map<Integer, ServerNode> serverNodes = new HashMap<>();
        for (Map.Entry<Integer, ServerInfo> entry :
                coordinatorContext.getLiveTabletServers().entrySet()) {
            ServerNode serverNode = entry.getValue().node(listenerName);
            if (serverNode != null) {
                serverNodes.put(entry.getKey(), serverNode);
            }
        }
        return serverNodes;
    }

    @Override
    public @Nullable ServerNode getCoordinatorServer(String listenerName) {
        ServerInfo coordinatorServer = coordinatorContext.getCoordinatorServerInfo();
        return coordinatorServer == null ? null : coordinatorServer.node(listenerName);
    }

    @Override
    public Set<Integer> getAliveTabletServerIds() {
        return Collections.unmodifiableSet(coordinatorContext.getLiveTabletServers().keySet());
    }

    @Override
    public Optional<TablePath> getTablePath(long tableId) {
        return Optional.ofNullable(coordinatorContext.getTablePathById(tableId));
    }

    public Optional<PhysicalTablePath> getPhysicalTablePath(long partitionId) {
        return coordinatorContext.getPhysicalTablePath(partitionId);
    }

    @Override
    public Optional<TableInfo> getTableInfo(long tableId) {
        return Optional.ofNullable(coordinatorContext.getTableInfoById(tableId));
    }

    @Override
    public Optional<TableMetadata> getTableMetadata(TablePath tablePath) {
        long tableId = coordinatorContext.getTableIdByPath(tablePath);
        if (tableId == TableInfo.UNKNOWN_TABLE_ID) {
            return Optional.empty();
        } else {
            TableInfo tableInfo = coordinatorContext.getTableInfoById(tableId);
            if (tableInfo != null) {
                List<BucketMetadata> bucketMetadataList =
                        getBucketMetadata(
                                tableId, null, coordinatorContext.getTableAssignment(tableId));
                return Optional.of(new TableMetadata(tableInfo, bucketMetadataList));
            } else {
                return Optional.empty();
            }
        }
    }

    @Override
    public Optional<PartitionMetadata> getPartitionMetadata(PhysicalTablePath partitionPath) {
        TablePath tablePath =
                new TablePath(partitionPath.getDatabaseName(), partitionPath.getTableName());
        String partitionName = partitionPath.getPartitionName();
        long tableId = coordinatorContext.getTableIdByPath(tablePath);
        if (tableId == TableInfo.UNKNOWN_TABLE_ID) {
            return Optional.empty();
        } else {
            Optional<Long> partitionIdOpt = coordinatorContext.getPartitionId(partitionPath);
            if (partitionIdOpt.isPresent()) {
                long partitionId = partitionIdOpt.get();
                List<BucketMetadata> bucketMetadataList =
                        getBucketMetadata(
                                tableId,
                                partitionId,
                                coordinatorContext.getPartitionAssignment(
                                        new TablePartition(tableId, partitionId)));
                return Optional.of(
                        new PartitionMetadata(
                                tableId, partitionName, partitionId, bucketMetadataList));
            } else {
                return Optional.empty();
            }
        }
    }

    private List<BucketMetadata> getBucketMetadata(
            long tableId, @Nullable Long partitionId, Map<Integer, List<Integer>> tableAssigment) {
        List<BucketMetadata> bucketMetadataList = new ArrayList<>();
        tableAssigment.forEach(
                (bucketId, serverIds) -> {
                    TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
                    Optional<LeaderAndIsr> optLeaderAndIsr =
                            coordinatorContext.getBucketLeaderAndIsr(tableBucket);
                    int leader = optLeaderAndIsr.map(LeaderAndIsr::leader).orElse(NO_LEADER);
                    BucketMetadata bucketMetadata =
                            new BucketMetadata(
                                    bucketId,
                                    leader,
                                    coordinatorContext.getBucketLeaderEpoch(tableBucket),
                                    serverIds);
                    bucketMetadataList.add(bucketMetadata);
                });
        return bucketMetadataList;
    }
}
