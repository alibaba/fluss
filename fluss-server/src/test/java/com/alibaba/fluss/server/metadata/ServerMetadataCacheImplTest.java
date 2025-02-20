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
import com.alibaba.fluss.rpc.messages.UpdateMetadataRequest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.alibaba.fluss.server.utils.RpcMessageUtils.makeTableBucketMetadata;
import static com.alibaba.fluss.server.utils.RpcMessageUtils.makeUpdateMetadataRequest;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ServerMetadataCacheImpl}. */
class ServerMetadataCacheImplTest {
    private ServerMetadataCache serverMetadataCache;
    private ServerNode coordinatorServer;
    private Set<ServerNode> aliveTableServers;

    @BeforeEach
    void setup() {
        serverMetadataCache = new ServerMetadataCacheImpl();
        coordinatorServer = new ServerNode(0, "localhost", 98, ServerType.COORDINATOR);
        aliveTableServers =
                new HashSet<>(
                        Arrays.asList(
                                new ServerNode(0, "localhost", 99, ServerType.TABLET_SERVER),
                                new ServerNode(1, "localhost", 100, ServerType.TABLET_SERVER),
                                new ServerNode(2, "localhost", 101, ServerType.TABLET_SERVER)));
    }

    @Test
    void testUpdateMetadataRequest() {
        // only update with cluster server metadata
        UpdateMetadataRequest updateMetadataRequest =
                makeUpdateMetadataRequest(Optional.of(coordinatorServer), aliveTableServers);
        serverMetadataCache.updateMetadata(updateMetadataRequest);
        verifyClusterServerMetadata(serverMetadataCache, coordinatorServer, aliveTableServers);

        Map<TableBucket, Integer> expectedLeaderByTableBucket = new HashMap<>();
        expectedLeaderByTableBucket.put(new TableBucket(0, 0), 0);
        expectedLeaderByTableBucket.put(new TableBucket(0, 1), 2);
        expectedLeaderByTableBucket.put(new TableBucket(0, 2), 1);
        expectedLeaderByTableBucket.put(new TableBucket(1, 0L, 1), 1);
        expectedLeaderByTableBucket.put(new TableBucket(1, 1L, 1), 2);
        expectedLeaderByTableBucket.put(new TableBucket(1, 2L, 2), 3);

        // update with table bucket metadata
        updateMetadataRequest.addAllBucketMetadatas(
                makeTableBucketMetadata(expectedLeaderByTableBucket));
        serverMetadataCache.updateMetadata(updateMetadataRequest);
        verifyTableBucketLeader(serverMetadataCache, expectedLeaderByTableBucket);

        // update with table bucket metadata again
        Map<TableBucket, Integer> newLeaderByTableBucket = new HashMap<>();
        newLeaderByTableBucket.put(new TableBucket(1, 0L, 1), 2);
        newLeaderByTableBucket.put(new TableBucket(2, 1L, 2), 3);
        updateMetadataRequest =
                new UpdateMetadataRequest()
                        .addAllBucketMetadatas(makeTableBucketMetadata(newLeaderByTableBucket));
        serverMetadataCache.updateMetadata(updateMetadataRequest);
        expectedLeaderByTableBucket.putAll(newLeaderByTableBucket);
        verifyTableBucketLeader(serverMetadataCache, expectedLeaderByTableBucket);

        // update with table and partition deleted
        updateMetadataRequest =
                new UpdateMetadataRequest()
                        .setDeletedTableIds(new long[] {0})
                        .setDeletedPartitionIds(new long[] {0, 1, 2});
        serverMetadataCache.updateMetadata(updateMetadataRequest);
        // all bucket leader info should be removed from cache
        expectedLeaderByTableBucket
                .keySet()
                .forEach(
                        (tableBucket) ->
                                assertThat(serverMetadataCache.getLeader(tableBucket)).isNull());
    }

    private void verifyClusterServerMetadata(
            ServerMetadataCache serverMetadataCache,
            ServerNode expectedCoordinatorServer,
            Set<ServerNode> expectedAliveTableServers) {
        assertThat(serverMetadataCache.getCoordinatorServer()).isEqualTo(expectedCoordinatorServer);
        assertThat(serverMetadataCache.isAliveTabletServer(0)).isTrue();
        assertThat(serverMetadataCache.getAllAliveTabletServers())
                .containsValues(expectedAliveTableServers.toArray(new ServerNode[0]));
    }

    private void verifyTableBucketLeader(
            ServerMetadataCache serverMetadataCache, Map<TableBucket, Integer> expectedLeaders) {
        for (Map.Entry<TableBucket, Integer> entry : expectedLeaders.entrySet()) {
            assertThat(serverMetadataCache.getLeader(entry.getKey())).isEqualTo(entry.getValue());
        }
    }
}
