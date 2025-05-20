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

package com.alibaba.fluss.server.testutils;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.metadata.PartitionMetadata;
import com.alibaba.fluss.server.metadata.ServerMetadataCache;
import com.alibaba.fluss.server.metadata.TableMetadata;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

/** An implement of {@link ServerMetadataCache} for testing purpose. */
public class TestingServerMetadataCache implements ServerMetadataCache {

    private final int[] serverIds;

    public TestingServerMetadataCache(int serverNums) {
        this.serverIds = IntStream.range(0, serverNums).toArray();
    }

    @Override
    public ServerNode getCoordinatorServer(String listenerName) {
        return null;
    }

    @Override
    public boolean isAliveTabletServer(int serverId) {
        return false;
    }

    @Override
    public Optional<ServerNode> getTabletServer(int serverId, String listenerName) {
        return Optional.empty();
    }

    @Override
    public Map<Integer, ServerNode> getAllAliveTabletServers(String listenerName) {
        return Collections.emptyMap();
    }

    @Override
    public Set<Integer> getAliveTabletServerIds() {
        return Collections.emptySet();
    }

    public int[] getLiveServerIds() {
        return serverIds;
    }

    @Override
    public Optional<TablePath> getTablePath(long tableId) {
        return Optional.empty();
    }

    @Override
    public Optional<PhysicalTablePath> getPhysicalTablePath(long partitionId) {
        return Optional.empty();
    }

    @Override
    public Optional<TableInfo> getTableInfo(long tableId) {
        return Optional.empty();
    }

    @Override
    public Optional<TableMetadata> getTableMetadata(TablePath tablePath) {
        return Optional.empty();
    }

    @Override
    public Optional<PartitionMetadata> getPartitionMetadata(PhysicalTablePath partitionPath) {
        return Optional.empty();
    }
}
