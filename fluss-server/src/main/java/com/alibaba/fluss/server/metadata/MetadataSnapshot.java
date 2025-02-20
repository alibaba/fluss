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

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** A snapshot of metadata of Fluss cluster that caches the necessary metadata the server needs. */
public class MetadataSnapshot {

    @Nullable private final ServerNode coordinatorServer;
    private final Map<Integer, ServerNode> aliveTabletServersById;

    // table id -> <bucket id -> leader server id>
    private final Map<Long, Map<Integer, Integer>> tableBucketLeaders;
    // table partition id -> <bucket id -> leader server id>
    private final Map<Long, Map<Integer, Integer>> tablePartitionBucketLeaders;

    public MetadataSnapshot(
            Map<Integer, ServerNode> aliveTabletServersById,
            @Nullable ServerNode coordinatorServer) {
        this.coordinatorServer = coordinatorServer;
        this.aliveTabletServersById = Collections.unmodifiableMap(aliveTabletServersById);

        this.tableBucketLeaders = new HashMap<>();
        this.tablePartitionBucketLeaders = new HashMap<>();
    }

    public MetadataSnapshot(
            Map<Integer, ServerNode> aliveTabletServersById,
            @Nullable ServerNode coordinatorServer,
            Map<Long, Map<Integer, Integer>> tableBucketLeaders,
            Map<Long, Map<Integer, Integer>> tablePartitionBucketLeaders) {
        this.coordinatorServer = coordinatorServer;
        this.aliveTabletServersById = aliveTabletServersById;
        this.tableBucketLeaders = tableBucketLeaders;
        this.tablePartitionBucketLeaders = tablePartitionBucketLeaders;
    }

    public MetadataSnapshot updateBucketLeaders(Map<TableBucket, Integer> bucketLeadersToUpdate) {
        Map<Long, Map<Integer, Integer>> newTableBucketLeaders = new HashMap<>(tableBucketLeaders);
        Map<Long, Map<Integer, Integer>> newTablePartitionBucketLeaders =
                new HashMap<>(tablePartitionBucketLeaders);
        for (Map.Entry<TableBucket, Integer> bucketAndLeader : bucketLeadersToUpdate.entrySet()) {
            TableBucket tableBucket = bucketAndLeader.getKey();
            Integer leaderServerId = bucketAndLeader.getValue();
            if (tableBucket.getPartitionId() == null) {
                newTableBucketLeaders
                        .computeIfAbsent(tableBucket.getTableId(), (tableId) -> new HashMap<>())
                        .put(tableBucket.getBucket(), leaderServerId);
            } else {
                newTablePartitionBucketLeaders
                        .computeIfAbsent(
                                tableBucket.getPartitionId(), (tablePartition) -> new HashMap<>())
                        .put(tableBucket.getBucket(), leaderServerId);
            }
        }

        return new MetadataSnapshot(
                aliveTabletServersById,
                coordinatorServer,
                newTableBucketLeaders,
                newTablePartitionBucketLeaders);
    }

    public MetadataSnapshot removeTableAndPartitions(long[] tableIds, long[] partitionIds) {
        Map<Long, Map<Integer, Integer>> newTableBucketLeaders = new HashMap<>(tableBucketLeaders);
        Map<Long, Map<Integer, Integer>> newTablePartitionBucketLeaders =
                new HashMap<>(tablePartitionBucketLeaders);
        for (Long tableId : tableIds) {
            newTableBucketLeaders.remove(tableId);
        }
        for (Long partitionId : partitionIds) {
            newTablePartitionBucketLeaders.remove(partitionId);
        }
        return new MetadataSnapshot(
                aliveTabletServersById,
                coordinatorServer,
                newTableBucketLeaders,
                newTablePartitionBucketLeaders);
    }

    public MetadataSnapshot updateClusterServers(
            Map<Integer, ServerNode> aliveTabletServersById,
            @Nullable ServerNode coordinatorServer) {
        return new MetadataSnapshot(
                aliveTabletServersById,
                coordinatorServer,
                tableBucketLeaders,
                tablePartitionBucketLeaders);
    }

    public Map<Integer, ServerNode> getAliveTabletServers() {
        return aliveTabletServersById;
    }

    /** Get alive tablet server by id. */
    public Optional<ServerNode> getAliveTabletServerById(int serverId) {
        return Optional.ofNullable(aliveTabletServersById.get(serverId));
    }

    public Integer getLeader(TableBucket tableBucket) {
        if (tableBucket.getPartitionId() == null) {
            Map<Integer, Integer> bucketLeaders = tableBucketLeaders.get(tableBucket.getTableId());
            return bucketLeaders == null ? null : bucketLeaders.get(tableBucket.getBucket());
        } else {
            Map<Integer, Integer> bucketLeaders =
                    tablePartitionBucketLeaders.get(tableBucket.getPartitionId());
            return bucketLeaders == null ? null : bucketLeaders.get(tableBucket.getBucket());
        }
    }

    @Nullable
    public ServerNode getCoordinatorServer() {
        return coordinatorServer;
    }

    /** Create an empty cluster instance with no nodes and no table-buckets. */
    public static MetadataSnapshot empty() {
        return new MetadataSnapshot(Collections.emptyMap(), null);
    }
}
