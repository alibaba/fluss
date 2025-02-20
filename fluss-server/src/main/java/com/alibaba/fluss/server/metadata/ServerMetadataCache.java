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

import com.alibaba.fluss.cluster.MetadataCache;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.messages.UpdateMetadataRequest;

import javax.annotation.Nullable;

import java.util.Map;

/** Metadata cache for server. it only caches the cluster metadata. */
public interface ServerMetadataCache extends MetadataCache {

    /**
     * Update the metadata by the remote update metadata request.
     *
     * @param updateMetadataRequest update metadata request
     */
    void updateMetadata(UpdateMetadataRequest updateMetadataRequest);

    /** Update the cluster servers metadata. */
    void updateClusterServers(
            ServerNode coordinatorServer, Map<Integer, ServerNode> aliveTabletServersById);

    /** Update the bucket leader in cache. */
    void updateBucketLeaders(Map<TableBucket, Integer> bucketLeaders);

    /** Delete the table from cache. */
    void deleteTable(long deletedTableId);

    /** Delete the partition from cache. */
    void deletePartition(long deletePartitionId);

    /** Return the leader of the table bucket in cache, null if the leader is not found in cache. */
    @Nullable
    Integer getLeader(TableBucket tableBucket);
}
