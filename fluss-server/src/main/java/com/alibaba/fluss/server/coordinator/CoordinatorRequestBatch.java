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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableBucketReplica;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.rpc.messages.NotifyKvSnapshotOffsetRequest;
import com.alibaba.fluss.rpc.messages.NotifyLakeTableOffsetRequest;
import com.alibaba.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import com.alibaba.fluss.rpc.messages.NotifyRemoteLogOffsetsRequest;
import com.alibaba.fluss.rpc.messages.PbNotifyLakeTableOffsetReqForBucket;
import com.alibaba.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
import com.alibaba.fluss.rpc.messages.PbStopReplicaReqForBucket;
import com.alibaba.fluss.rpc.messages.PbStopReplicaRespForBucket;
import com.alibaba.fluss.rpc.messages.StopReplicaRequest;
import com.alibaba.fluss.rpc.messages.UpdateMetadataRequest;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.server.coordinator.event.DeleteReplicaResponseReceivedEvent;
import com.alibaba.fluss.server.coordinator.event.EventManager;
import com.alibaba.fluss.server.coordinator.event.NotifyLeaderAndIsrResponseReceivedEvent;
import com.alibaba.fluss.server.entity.DeleteReplicaResultForBucket;
import com.alibaba.fluss.server.entity.NotifyLeaderAndIsrData;
import com.alibaba.fluss.server.metadata.BucketMetadata;
import com.alibaba.fluss.server.metadata.PartitionMetadata;
import com.alibaba.fluss.server.metadata.TableMetadata;
import com.alibaba.fluss.server.zk.data.LakeTableSnapshot;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.fluss.server.metadata.PartitionMetadata.PARTITION_DURATION_DELETE_ID;
import static com.alibaba.fluss.server.metadata.TableMetadata.TABLE_DURATION_DELETE_ID;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getNotifyLeaderAndIsrResponseData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeNotifyBucketLeaderAndIsr;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeNotifyKvSnapshotOffsetRequest;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeNotifyLakeTableOffsetForBucket;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeNotifyLeaderAndIsrRequest;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeNotifyRemoteLogOffsetsRequest;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeStopBucketReplica;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeUpdateMetadataRequest;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.toTableBucket;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** A request sender for coordinator server to request to tablet server by batch. */
public class CoordinatorRequestBatch {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorRequestBatch.class);

    // a map from tablet server to notify the leader and isr for each bucket.
    private final Map<Integer, Map<TableBucket, PbNotifyLeaderAndIsrReqForBucket>>
            notifyLeaderAndIsrRequestMap = new HashMap<>();
    // a map from tablet server to stop replica for each bucket.
    private final Map<Integer, Map<TableBucket, PbStopReplicaReqForBucket>> stopReplicaRequestMap =
            new HashMap<>();

    // a set of tabletServers to send update metadata request.
    private final Set<Integer> updateMetadataRequestTabletServerSet = new HashSet<>();
    // a map from tableId to bucket metadata to update.
    private final Map<Long, List<BucketMetadata>> updateMetadataRequestBucketMap = new HashMap<>();
    // a map from tableId to (a map from partitionId to bucket metadata) to update.
    private final Map<Long, Map<Long, List<BucketMetadata>>> updateMetadataRequestPartitionMap =
            new HashMap<>();

    // a map from tablet server to notify remote log offsets request.
    private final Map<Integer, NotifyRemoteLogOffsetsRequest> notifyRemoteLogOffsetsRequestMap =
            new HashMap<>();
    // a map from tablet server to notify kv snapshot offset request.
    private final Map<Integer, NotifyKvSnapshotOffsetRequest> notifyKvSnapshotOffsetRequestMap =
            new HashMap<>();

    private final Map<Integer, Map<TableBucket, PbNotifyLakeTableOffsetReqForBucket>>
            notifyLakeTableOffsetRequestMap = new HashMap<>();

    private final CoordinatorChannelManager coordinatorChannelManager;
    private final EventManager eventManager;
    private final CoordinatorContext coordinatorContext;

    public CoordinatorRequestBatch(
            CoordinatorChannelManager coordinatorChannelManager,
            EventManager eventManager,
            CoordinatorContext coordinatorContext) {
        this.coordinatorChannelManager = coordinatorChannelManager;
        this.eventManager = eventManager;
        this.coordinatorContext = coordinatorContext;
    }

    public void newBatch() {
        if (!notifyLeaderAndIsrRequestMap.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "The NotifyLeaderAndIsr batch request from coordinator to tablet server is not empty while creating "
                                    + "a new one. Some NotifyLeaderAndIsr request in %s might be lost.",
                            notifyLeaderAndIsrRequestMap));
        }
        if (!stopReplicaRequestMap.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "The StopReplica batch request from coordinator to tablet server is not empty while creating "
                                    + "a new one. Some StopReplica request in %s might be lost.",
                            stopReplicaRequestMap));
        }
        if (!updateMetadataRequestTabletServerSet.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "The UpdateMetadata request from coordinator to tablet server is not empty while creating "
                                    + "a new one. Some UpdateMetadata request in %s might be lost.",
                            updateMetadataRequestTabletServerSet));
        }

        if (!notifyRemoteLogOffsetsRequestMap.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "The DeleteLogSegments request from coordinator to tablet server is not empty while creating "
                                    + "a new one. Some DeleteLogSegments request in %s might be lost.",
                            notifyRemoteLogOffsetsRequestMap));
        }

        if (!notifyLakeTableOffsetRequestMap.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "The NotifyLakeTableOffset request from coordinator to tablet server is not empty while creating "
                                    + "a new one. Some NotifyLakeTableOffset request in %s might be lost.",
                            notifyLakeTableOffsetRequestMap));
        }
    }

    public void sendRequestToTabletServers(int coordinatorEpoch) {
        try {
            sendNotifyLeaderAndIsrRequest(coordinatorEpoch);
            sendUpdateMetadataRequest();
            sendNotifyRemoteLogOffsetsRequest(coordinatorEpoch);
            sendNotifyKvSnapshotOffsetRequest(coordinatorEpoch);
            sendStopRequest(coordinatorEpoch);
        } catch (Throwable t) {
            if (!notifyLeaderAndIsrRequestMap.isEmpty()) {
                LOG.error(
                        "Haven't been able to send notify leader and isr requests, current state of the map is {}.",
                        notifyLeaderAndIsrRequestMap,
                        t);
            }
            if (!updateMetadataRequestTabletServerSet.isEmpty()) {
                LOG.error(
                        "Haven't been able to send update metadata requests, current state of the map is {}.",
                        updateMetadataRequestTabletServerSet,
                        t);
            }
            if (!stopReplicaRequestMap.isEmpty()) {
                LOG.error(
                        "Haven't been able to send stop replica requests, current state of the map is {}.",
                        stopReplicaRequestMap,
                        t);
            }
            if (!notifyRemoteLogOffsetsRequestMap.isEmpty()) {
                LOG.error(
                        "Haven't been able to send delete log segments requests, current state of the map is {}.",
                        notifyRemoteLogOffsetsRequestMap,
                        t);
            }
            if (!notifyLakeTableOffsetRequestMap.isEmpty()) {
                LOG.error(
                        "Haven't been able to send notify lake house data requests, current state of the map is {}.",
                        notifyLakeTableOffsetRequestMap,
                        t);
            }
            throw new IllegalStateException(t);
        }
    }

    public void addNotifyLeaderRequestForTabletServers(
            Set<Integer> tabletServers,
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            List<Integer> bucketReplicas,
            LeaderAndIsr leaderAndIsr) {
        tabletServers.stream()
                .filter(s -> s >= 0)
                .forEach(
                        id -> {
                            Map<TableBucket, PbNotifyLeaderAndIsrReqForBucket>
                                    notifyBucketLeaderAndIsr =
                                            notifyLeaderAndIsrRequestMap.computeIfAbsent(
                                                    id, k -> new HashMap<>());
                            PbNotifyLeaderAndIsrReqForBucket notifyLeaderAndIsrForBucket =
                                    makeNotifyBucketLeaderAndIsr(
                                            new NotifyLeaderAndIsrData(
                                                    tablePath,
                                                    tableBucket,
                                                    bucketReplicas,
                                                    leaderAndIsr));
                            notifyBucketLeaderAndIsr.put(tableBucket, notifyLeaderAndIsrForBucket);
                        });

        addUpdateMetadataRequestForTabletServers(
                coordinatorContext.getLiveTabletServers().keySet(),
                Collections.singleton(tableBucket));
    }

    public void addStopReplicaRequestForTabletServers(
            Set<Integer> tabletServers,
            TableBucket tableBucket,
            boolean isDelete,
            int leaderEpoch) {
        tabletServers.stream()
                .filter(s -> s >= 0)
                .forEach(
                        id -> {
                            Map<TableBucket, PbStopReplicaReqForBucket> stopBucketReplica =
                                    stopReplicaRequestMap.computeIfAbsent(id, k -> new HashMap<>());
                            // reduce delete flag, if it has been marked as deleted,
                            // we will set it as delete replica
                            boolean alreadyDelete =
                                    stopBucketReplica.get(tableBucket) != null
                                            && stopBucketReplica.get(tableBucket).isDelete();
                            PbStopReplicaReqForBucket protoStopReplicaForBucket =
                                    makeStopBucketReplica(
                                            tableBucket, alreadyDelete || isDelete, leaderEpoch);
                            stopBucketReplica.put(tableBucket, protoStopReplicaForBucket);
                        });
    }

    /**
     * Add update metadata request for tablet servers when create/drop table or create/drop
     * partition.
     */
    public void addUpdateMetadataRequestForTabletServers(
            Set<Integer> tabletServers, @Nullable Long tableId, @Nullable Long partitionId) {
        tabletServers.stream()
                .filter(s -> s >= 0)
                .forEach(updateMetadataRequestTabletServerSet::add);

        if (tableId != null) {
            updateMetadataRequestBucketMap.put(tableId, Collections.emptyList());
        }

        if (partitionId != null) {
            updateMetadataRequestPartitionMap
                    .computeIfAbsent(tableId, k -> new HashMap<>())
                    .put(partitionId, Collections.emptyList());
        }
    }

    /**
     * Add update metadata request for tablet servers when table bucket leader and isr is changed.
     */
    private void addUpdateMetadataRequestForTabletServers(
            Set<Integer> tabletServers, Set<TableBucket> tableBuckets) {
        tabletServers.stream()
                .filter(s -> s >= 0)
                .forEach(updateMetadataRequestTabletServerSet::add);

        for (TableBucket tableBucket : tableBuckets) {
            long currentTableId = tableBucket.getTableId();
            Optional<LeaderAndIsr> bucketLeaderAndIsr =
                    coordinatorContext.getBucketLeaderAndIsr(tableBucket);
            if (bucketLeaderAndIsr.isPresent()) {
                LeaderAndIsr leaderAndIsr = bucketLeaderAndIsr.get();
                BucketMetadata bucketMetadata =
                        new BucketMetadata(
                                tableBucket.getBucket(),
                                leaderAndIsr.leader(),
                                leaderAndIsr.leaderEpoch(),
                                coordinatorContext.getAssignment(tableBucket));
                if (tableBucket.getPartitionId() != null) {
                    updateMetadataRequestPartitionMap
                            .computeIfAbsent(currentTableId, k -> new HashMap<>())
                            .computeIfAbsent(tableBucket.getPartitionId(), k -> new ArrayList<>())
                            .add(bucketMetadata);
                } else {
                    updateMetadataRequestBucketMap
                            .computeIfAbsent(currentTableId, k -> new ArrayList<>())
                            .add(bucketMetadata);
                }
            }
        }
    }

    public void addNotifyRemoteLogOffsetsRequestForTabletServers(
            List<Integer> tabletServers,
            TableBucket tableBucket,
            long remoteLogStartOffset,
            long remoteLogEndOffset) {
        tabletServers.stream()
                .filter(s -> s >= 0)
                .forEach(
                        id ->
                                notifyRemoteLogOffsetsRequestMap.put(
                                        id,
                                        makeNotifyRemoteLogOffsetsRequest(
                                                tableBucket,
                                                remoteLogStartOffset,
                                                remoteLogEndOffset)));
    }

    public void addNotifyKvSnapshotOffsetRequestForTabletServers(
            List<Integer> tabletServers, TableBucket tableBucket, long minRetainOffset) {
        tabletServers.stream()
                .filter(s -> s >= 0)
                .forEach(
                        id ->
                                notifyKvSnapshotOffsetRequestMap.put(
                                        id,
                                        makeNotifyKvSnapshotOffsetRequest(
                                                tableBucket, minRetainOffset)));
    }

    public void addNotifyLakeTableOffsetRequestForTableServers(
            List<Integer> tabletServers,
            TableBucket tableBucket,
            LakeTableSnapshot lakeTableSnapshot) {
        tabletServers.stream()
                .filter(s -> s >= 0)
                .forEach(
                        id -> {
                            Map<TableBucket, PbNotifyLakeTableOffsetReqForBucket>
                                    notifyLakeTableOffsetReqForBucketMap =
                                            notifyLakeTableOffsetRequestMap.computeIfAbsent(
                                                    id, k -> new HashMap<>());
                            notifyLakeTableOffsetReqForBucketMap.put(
                                    tableBucket,
                                    makeNotifyLakeTableOffsetForBucket(
                                            tableBucket, lakeTableSnapshot));
                        });
    }

    private void sendNotifyLeaderAndIsrRequest(int coordinatorEpoch) {
        for (Map.Entry<Integer, Map<TableBucket, PbNotifyLeaderAndIsrReqForBucket>>
                notifyRequestEntry : notifyLeaderAndIsrRequestMap.entrySet()) {
            // send request for each tablet server
            Integer serverId = notifyRequestEntry.getKey();
            NotifyLeaderAndIsrRequest notifyLeaderAndIsrRequest =
                    makeNotifyLeaderAndIsrRequest(
                            coordinatorEpoch, notifyRequestEntry.getValue().values());

            coordinatorChannelManager.sendBucketLeaderAndIsrRequest(
                    serverId,
                    notifyLeaderAndIsrRequest,
                    (response, throwable) -> {
                        if (throwable != null) {
                            LOG.warn(
                                    "Failed to send notify leader and isr request to tablet server {}.",
                                    serverId,
                                    throwable);
                            // todo: in FLUSS-55886145, we will introduce a sender thread to send
                            // the request, and retry if encounter any error; It may happens that
                            // the tablet server is offline and will always got error. But,
                            // coordinator will remove the sender for the tablet server and mark all
                            // replica in the tablet server as offline. so, in here, if encounter
                            // any error, we just ignore it.
                            return;
                        }
                        // put the response receive event into the event manager
                        eventManager.put(
                                new NotifyLeaderAndIsrResponseReceivedEvent(
                                        getNotifyLeaderAndIsrResponseData(response), serverId));
                    });
        }
        notifyLeaderAndIsrRequestMap.clear();
    }

    private void sendStopRequest(int coordinatorEpoch) {
        for (Map.Entry<Integer, Map<TableBucket, PbStopReplicaReqForBucket>> stopReplciaEntry :
                stopReplicaRequestMap.entrySet()) {
            // send request for each tablet server
            Integer serverId = stopReplciaEntry.getKey();

            // construct the stop replica request
            Map<TableBucket, PbStopReplicaReqForBucket> stopReplicas = stopReplciaEntry.getValue();
            StopReplicaRequest stopReplicaRequest = new StopReplicaRequest();
            stopReplicaRequest
                    .setCoordinatorEpoch(coordinatorEpoch)
                    .addAllStopReplicasReqs(stopReplicas.values());

            // we collect the buckets whose replica is to be deleted
            Set<TableBucket> deletedReplicaBuckets =
                    stopReplicas.values().stream()
                            .filter(PbStopReplicaReqForBucket::isDelete)
                            .map(t -> toTableBucket(t.getTableBucket()))
                            .collect(Collectors.toSet());

            coordinatorChannelManager.sendStopBucketReplicaRequest(
                    serverId,
                    stopReplicaRequest,
                    (response, throwable) -> {
                        if (throwable != null) {
                            // todo: in FLUSS-55886145, we will introduce a sender thread to send
                            // the request.
                            // in here, we just ignore the error.
                            LOG.warn(
                                    "Failed to send stop replica request to tablet server {}.",
                                    serverId,
                                    throwable);
                            return;
                        }
                        // handle the response
                        List<DeleteReplicaResultForBucket> deleteReplicaResultForBuckets =
                                new ArrayList<>();
                        List<PbStopReplicaRespForBucket> stopReplicasResps =
                                response.getStopReplicasRespsList();
                        // construct the result for stop replica
                        // for each replica
                        for (PbStopReplicaRespForBucket stopReplicaRespForBucket :
                                stopReplicasResps) {
                            TableBucket tableBucket =
                                    toTableBucket(stopReplicaRespForBucket.getTableBucket());

                            // now, for stop replica(delete=false), it's best effort without any
                            // error handling.
                            // currently, it only happens in the two case:
                            // 1. send stop replica(delete = false) for table deletion, if it fails,
                            // the following step will trigger replica to ReplicaDeletionStarted
                            // will send stop replica(delete =true) which will retry if fail.
                            // 2. send notify leader and isr request to tablet server, but the
                            // tablet server fail to init a replica
                            // then, it'll send stop replica(delete = false) to the tablet server to
                            // make the tablet server can stop the replica; It's still fine if
                            // sending stop replica fail.
                            // todo: let's revisit here to see whether we can
                            // really ignore the error after
                            // we finish the logic of tablet server.

                            // but for stop replica(delete=true), we need to handle the error and
                            // retry deletion.

                            // filter out the  error response for replica deletion.
                            if (deletedReplicaBuckets.contains(tableBucket)) {
                                DeleteReplicaResultForBucket deleteReplicaResultForBucket;
                                TableBucketReplica tableBucketReplica =
                                        new TableBucketReplica(tableBucket, serverId);
                                // if fail;
                                if (stopReplicaRespForBucket.hasErrorCode()) {
                                    deleteReplicaResultForBucket =
                                            new DeleteReplicaResultForBucket(
                                                    tableBucketReplica.getTableBucket(),
                                                    serverId,
                                                    ApiError.fromErrorMessage(
                                                            stopReplicaRespForBucket));
                                } else {
                                    deleteReplicaResultForBucket =
                                            new DeleteReplicaResultForBucket(tableBucket, serverId);
                                }
                                deleteReplicaResultForBuckets.add(deleteReplicaResultForBucket);
                            }
                        }
                        // if there are any deleted replicas, construct
                        // the DeleteReplicaResponseReceivedEvent and put into event manager
                        if (!deleteReplicaResultForBuckets.isEmpty()) {
                            DeleteReplicaResponseReceivedEvent deleteReplicaResponseReceivedEvent =
                                    new DeleteReplicaResponseReceivedEvent(
                                            deleteReplicaResultForBuckets);
                            eventManager.put(deleteReplicaResponseReceivedEvent);
                        }
                    });
        }
        stopReplicaRequestMap.clear();
    }

    public void sendUpdateMetadataRequest() {
        // Build updateMetadataRequest.
        UpdateMetadataRequest updateMetadataRequest = buildUpdateMetadataRequest();
        for (Integer serverId : updateMetadataRequestTabletServerSet) {
            coordinatorChannelManager.sendUpdateMetadataRequest(
                    serverId,
                    updateMetadataRequest,
                    (response, throwable) -> {
                        if (throwable != null) {
                            LOG.debug("Failed to send update metadata request.", throwable);
                        } else {
                            LOG.debug("Update metadata for server {} success.", serverId);
                        }
                    });
        }
        updateMetadataRequestTabletServerSet.clear();
        updateMetadataRequestBucketMap.clear();
        updateMetadataRequestPartitionMap.clear();
    }

    public void sendNotifyRemoteLogOffsetsRequest(int coordinatorEpoch) {
        for (Map.Entry<Integer, NotifyRemoteLogOffsetsRequest> notifyRemoteLogOffsetsRequestEntry :
                notifyRemoteLogOffsetsRequestMap.entrySet()) {
            Integer serverId = notifyRemoteLogOffsetsRequestEntry.getKey();
            NotifyRemoteLogOffsetsRequest notifyRemoteLogOffsetsRequest =
                    notifyRemoteLogOffsetsRequestEntry.getValue();
            notifyRemoteLogOffsetsRequest.setCoordinatorEpoch(coordinatorEpoch);
            coordinatorChannelManager.sendNotifyRemoteLogOffsetsRequest(
                    serverId,
                    notifyRemoteLogOffsetsRequest,
                    (response, throwable) -> {
                        if (throwable != null) {
                            LOG.warn(
                                    "Failed to send notify remote log offsets request.", throwable);
                        } else {
                            LOG.debug("Notify remote log offsets for server {} success.", serverId);
                        }
                    });
        }
        notifyRemoteLogOffsetsRequestMap.clear();
    }

    public void sendNotifyKvSnapshotOffsetRequest(int coordinatorEpoch) {
        for (Map.Entry<Integer, NotifyKvSnapshotOffsetRequest> notifySnapshotOffsetRequestEntry :
                notifyKvSnapshotOffsetRequestMap.entrySet()) {
            Integer serverId = notifySnapshotOffsetRequestEntry.getKey();
            NotifyKvSnapshotOffsetRequest notifySnapshotOffsetRequest =
                    notifySnapshotOffsetRequestEntry.getValue();
            notifySnapshotOffsetRequest.setCoordinatorEpoch(coordinatorEpoch);
            coordinatorChannelManager.sendNotifyKvSnapshotOffsetRequest(
                    serverId,
                    notifySnapshotOffsetRequest,
                    (response, throwable) -> {
                        if (throwable != null) {
                            LOG.warn("Failed to send notify snapshot offset request.", throwable);
                        } else {
                            LOG.debug("Notify snapshot offset for server {} success.", serverId);
                        }
                    });
        }
        notifyKvSnapshotOffsetRequestMap.clear();
    }

    public void sendNotifyLakeTableOffsetRequest(int coordinatorEpoch) {
        for (Map.Entry<Integer, Map<TableBucket, PbNotifyLakeTableOffsetReqForBucket>>
                notifyLakeTableOffsetEntry : notifyLakeTableOffsetRequestMap.entrySet()) {
            Integer serverId = notifyLakeTableOffsetEntry.getKey();
            Map<TableBucket, PbNotifyLakeTableOffsetReqForBucket> notifyLogOffsets =
                    notifyLakeTableOffsetEntry.getValue();

            NotifyLakeTableOffsetRequest notifyLakeTableOffsetRequest =
                    new NotifyLakeTableOffsetRequest()
                            .setCoordinatorEpoch(coordinatorEpoch)
                            .addAllNotifyBucketsReqs(notifyLogOffsets.values());

            coordinatorChannelManager.sendNotifyLakeTableOffsetRequest(
                    serverId,
                    notifyLakeTableOffsetRequest,
                    (response, throwable) -> {
                        if (throwable != null) {
                            LOG.warn("Failed to send notify lake table offset.", throwable);
                        } else {
                            LOG.debug("Notify lake table offset for server {} success.", serverId);
                        }
                    });
        }
        notifyLakeTableOffsetRequestMap.clear();
    }

    private UpdateMetadataRequest buildUpdateMetadataRequest() {
        List<TableMetadata> tableMetadataList = new ArrayList<>();
        updateMetadataRequestBucketMap.forEach(
                (tableId, bucketMetadataList) -> {
                    TableInfo tableInfo = coordinatorContext.getTableInfoById(tableId);
                    checkNotNull(tableInfo, "Table info is null.");
                    TableInfo newTableInfo =
                            coordinatorContext.isTableQueuedForDeletion(tableId)
                                    ? TableInfo.of(
                                            tableInfo.getTablePath(),
                                            TABLE_DURATION_DELETE_ID,
                                            tableInfo.getSchemaId(),
                                            tableInfo.toTableDescriptor(),
                                            tableInfo.getCreatedTime(),
                                            tableInfo.getModifiedTime())
                                    : tableInfo;
                    tableMetadataList.add(new TableMetadata(newTableInfo, bucketMetadataList));
                });

        List<PartitionMetadata> partitionMetadataList = new ArrayList<>();
        updateMetadataRequestPartitionMap.forEach(
                (tableId, partitionIdToBucketMetadataMap) -> {
                    for (Map.Entry<Long, List<BucketMetadata>> kvEntry :
                            partitionIdToBucketMetadataMap.entrySet()) {
                        Long partitionId = kvEntry.getKey();
                        Long newPartitionId =
                                coordinatorContext.isPartitionQueuedForDeletion(
                                                new TablePartition(tableId, partitionId))
                                        ? PARTITION_DURATION_DELETE_ID
                                        : partitionId;
                        String partitionName = coordinatorContext.getPartitionName(partitionId);
                        checkNotNull(partitionName, "Partition name is null.");
                        partitionMetadataList.add(
                                new PartitionMetadata(
                                        tableId,
                                        partitionName,
                                        newPartitionId,
                                        kvEntry.getValue()));
                    }
                });
        return makeUpdateMetadataRequest(
                coordinatorContext.getCoordinatorServerInfo(),
                new HashSet<>(coordinatorContext.getLiveTabletServers().values()),
                tableMetadataList,
                partitionMetadataList);
    }
}
