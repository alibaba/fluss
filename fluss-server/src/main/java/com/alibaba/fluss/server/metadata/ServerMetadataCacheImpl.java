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
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.messages.PbBucketMetadata;
import com.alibaba.fluss.rpc.messages.PbClusterServerMetadata;
import com.alibaba.fluss.rpc.messages.PbServerNode;
import com.alibaba.fluss.rpc.messages.PbTableBucketMetadata;
import com.alibaba.fluss.rpc.messages.UpdateMetadataRequest;
import com.alibaba.fluss.server.coordinator.CoordinatorServer;
import com.alibaba.fluss.server.tablet.TabletServer;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.server.utils.RpcMessageUtils.toServerNode;
import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/**
 * The server metadata cache to cache the cluster metadata info needs in server. This cache is
 * updated through UpdateMetadataRequest from the {@link CoordinatorServer}. {@link
 * CoordinatorServer} and each {@link TabletServer} maintains the same cache, asynchronously.
 */
public class ServerMetadataCacheImpl implements ServerMetadataCache {

    private final Lock bucketMetadataLock = new ReentrantLock();

    /**
     * This is cache state. every metadata snapshot instance is immutable, and updates (performed
     * under a lock) replace the value with a completely new one. this means reads (which are not
     * under any lock) need to grab the value of this ONCE and retain that read copy for the
     * duration of their operation.
     *
     * <p>multiple reads of this value risk getting different snapshots.
     */
    protected volatile MetadataSnapshot metadataSnapshot;

    public ServerMetadataCacheImpl() {
        this.metadataSnapshot = MetadataSnapshot.empty();
    }

    @Override
    public void updateMetadata(UpdateMetadataRequest updateMetadataRequest) {
        inLock(
                bucketMetadataLock,
                () -> {
                    MetadataSnapshot newMetadataSnapshot = metadataSnapshot;
                    if (updateMetadataRequest.hasClusterServerMetadata()) {
                        PbClusterServerMetadata pbClusterServerMetadata =
                                updateMetadataRequest.getClusterServerMetadata();
                        ServerNode coordinatorServer =
                                pbClusterServerMetadata.hasCoordinatorServer()
                                        ? toServerNode(
                                                pbClusterServerMetadata.getCoordinatorServer(),
                                                ServerType.COORDINATOR)
                                        : null;

                        // 2. Update the alive table servers. We always use the new alive table
                        // servers
                        // to replace the old alive table servers.
                        HashMap<Integer, ServerNode> newAliveTableServers = new HashMap<>();
                        for (PbServerNode pbServerNode :
                                pbClusterServerMetadata.getTabletServersList()) {
                            newAliveTableServers.put(
                                    pbServerNode.getNodeId(),
                                    toServerNode(pbServerNode, ServerType.TABLET_SERVER));
                        }
                        newMetadataSnapshot =
                                newMetadataSnapshot.updateClusterServers(
                                        newAliveTableServers, coordinatorServer);
                    }

                    Map<TableBucket, Integer> bucketLeaders = new HashMap<>();
                    for (PbTableBucketMetadata pbTableBucketMetadata :
                            updateMetadataRequest.getBucketMetadatasList()) {
                        long tableId = pbTableBucketMetadata.getTableId();
                        Long partitionId =
                                pbTableBucketMetadata.hasPartitionId()
                                        ? pbTableBucketMetadata.getPartitionId()
                                        : null;
                        for (PbBucketMetadata pbBucketMetadata :
                                pbTableBucketMetadata.getBucketMetadatasList()) {
                            bucketLeaders.put(
                                    new TableBucket(
                                            tableId, partitionId, pbBucketMetadata.getBucketId()),
                                    pbBucketMetadata.getLeaderId());
                        }
                    }

                    if (!bucketLeaders.isEmpty()) {
                        newMetadataSnapshot =
                                newMetadataSnapshot.updateBucketLeaders(bucketLeaders);
                    }

                    long[] deletedTableIds = updateMetadataRequest.getDeletedTableIds();
                    long[] deletedPartitionIds = updateMetadataRequest.getDeletedPartitionIds();

                    if (deletedTableIds.length > 0 || deletedPartitionIds.length > 0) {
                        newMetadataSnapshot =
                                newMetadataSnapshot.removeTableAndPartitions(
                                        deletedTableIds, deletedPartitionIds);
                    }

                    if (newMetadataSnapshot != metadataSnapshot) {
                        metadataSnapshot = newMetadataSnapshot;
                    }
                });
    }

    @Override
    public boolean isAliveTabletServer(int serverId) {
        Map<Integer, ServerNode> aliveTabletServersById = metadataSnapshot.getAliveTabletServers();
        return aliveTabletServersById.containsKey(serverId);
    }

    @Override
    public @Nullable ServerNode getTabletServer(int serverId) {
        return metadataSnapshot.getAliveTabletServerById(serverId).orElse(null);
    }

    @Override
    public Map<Integer, ServerNode> getAllAliveTabletServers() {
        return metadataSnapshot.getAliveTabletServers();
    }

    @Override
    public @Nullable ServerNode getCoordinatorServer() {
        return metadataSnapshot.getCoordinatorServer();
    }

    @Override
    public void updateClusterServers(
            ServerNode coordinatorServer, Map<Integer, ServerNode> aliveTabletServersById) {
        inLock(
                bucketMetadataLock,
                () -> {
                    metadataSnapshot =
                            metadataSnapshot.updateClusterServers(
                                    aliveTabletServersById, coordinatorServer);
                });
    }

    @Override
    public void updateBucketLeaders(Map<TableBucket, Integer> bucketLeaders) {
        inLock(
                bucketMetadataLock,
                () -> {
                    metadataSnapshot = metadataSnapshot.updateBucketLeaders(bucketLeaders);
                });
    }

    @Override
    public void deleteTable(long tableId) {
        inLock(
                bucketMetadataLock,
                () -> {
                    metadataSnapshot =
                            metadataSnapshot.removeTableAndPartitions(
                                    new long[] {tableId}, new long[0]);
                });
    }

    @Override
    public void deletePartition(long partitionId) {
        inLock(
                bucketMetadataLock,
                () -> {
                    metadataSnapshot =
                            metadataSnapshot.removeTableAndPartitions(
                                    new long[0], new long[] {partitionId});
                });
    }

    @Nullable
    @Override
    public Integer getLeader(TableBucket tableBucket) {
        return metadataSnapshot.getLeader(tableBucket);
    }
}
