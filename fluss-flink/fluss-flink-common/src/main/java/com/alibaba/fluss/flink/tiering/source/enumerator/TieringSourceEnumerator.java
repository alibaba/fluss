/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.tiering.source.enumerator;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.metrics.FlinkMetricRegistry;
import com.alibaba.fluss.flink.tiering.source.split.TieringSplit;
import com.alibaba.fluss.flink.tiering.source.state.TieringSourceEnumeratorState;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import com.alibaba.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import com.alibaba.fluss.rpc.messages.PbHeartbeatReqForTable;
import com.alibaba.fluss.rpc.messages.PbLakeTieringTableInfo;
import com.alibaba.fluss.rpc.metrics.ClientMetricGroup;
import com.alibaba.fluss.utils.types.Tuple2;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.Preconditions.checkState;

/**
 * An implementation of {@link SplitEnumerator} used to request {@link TieringSplit} from Fluss
 * Cluster.
 *
 * <p>The enumerator is responsible for:
 *
 * <ul>
 *   <li>Register the Tiering Service job that the current TieringSourceEnumerator belongs to with
 *       the Fluss Cluster when the Flink Tiering job starts up.
 *   <li>Request Fluss table splits from Fluss Cluster and assigns to SourceReader to tier.
 *   <li>Un-Register the Tiering Service job that the current TieringSourceEnumerator belongs to
 *       with the Fluss Cluster when the Flink Tiering job shutdown as much as possible.
 * </ul>
 */
public class TieringSourceEnumerator
        implements SplitEnumerator<TieringSplit, TieringSourceEnumeratorState> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TieringSourceEnumerator.class);

    private final Configuration flussConf;
    private final SplitEnumeratorContext<TieringSplit> context;
    private final SplitEnumeratorMetricGroup enumeratorMetricGroup;
    private final long rpcRequestTimeoutMs;
    private final List<TieringSplit> pendingSplits;
    private final Set<Integer> readersAwaitingSplit;

    // lazily instantiated
    private RpcClient rpcClient;
    private CoordinatorGateway coordinatorGateway;
    private Connection connection;
    private Admin flussAdmin;
    private long tieringServiceEpoch;
    private long coordinatorEpoch;

    public TieringSourceEnumerator(
            Configuration flussConf,
            SplitEnumeratorContext<TieringSplit> context,
            SplitEnumeratorMetricGroup enumeratorMetricGroup,
            long rpcRequestTimeoutMs) {
        this.flussConf = flussConf;
        this.context = context;
        this.enumeratorMetricGroup = enumeratorMetricGroup;
        this.rpcRequestTimeoutMs = rpcRequestTimeoutMs;
        this.pendingSplits = new ArrayList<>();
        this.readersAwaitingSplit = new TreeSet<>();
    }

    @Override
    public void start() {
        connection = ConnectionFactory.createConnection(flussConf);
        flussAdmin = connection.getAdmin();
        FlinkMetricRegistry metricRegistry = new FlinkMetricRegistry(enumeratorMetricGroup);
        ClientMetricGroup clientMetricGroup =
                new ClientMetricGroup(metricRegistry, "LakeTieringService");
        this.rpcClient = RpcClient.create(flussConf, clientMetricGroup);
        MetadataUpdater metadataUpdater = new MetadataUpdater(flussConf, rpcClient);
        this.coordinatorGateway =
                GatewayClientProxy.createGatewayProxy(
                        metadataUpdater::getCoordinatorServer, rpcClient, CoordinatorGateway.class);
        this.tieringServiceEpoch = System.currentTimeMillis();

        LOGGER.info(
                "Starting register Tiering Service(epoch={}) to Fluss Coordinator...",
                tieringServiceEpoch);
        try {
            LakeTieringHeartbeatResponse heartbeatResponse =
                    HeartBeatHelper.waitHeartbeatResponse(
                            coordinatorGateway.lakeTieringHeartbeat(
                                    HeartBeatHelper.registerHeartBeat()),
                            rpcRequestTimeoutMs);
            this.coordinatorEpoch = heartbeatResponse.getCoordinatorEpoch();
            LOGGER.info(
                    "Register Tiering Service(epoch={}) to Fluss Coordinator(epoch={}) ",
                    tieringServiceEpoch,
                    coordinatorEpoch);

        } catch (Exception e) {
            LOGGER.error(
                    "Register Tiering Service(epoch={}) failed due to ", tieringServiceEpoch, e);
            throw new FlinkRuntimeException(
                    "Register Tiering Service(epoch=" + tieringServiceEpoch + ") failed due to ",
                    e);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader may be failed, skip this request.
            return;
        }
        LOGGER.info("TiringSourceReader {} requests split.", subtaskId);
        readersAwaitingSplit.add(subtaskId);
        this.context.callAsync(this::requestTieringTableSplitsViaHeartBeat, this::assignSplits);
    }

    private void assignSplits(Tuple2<Long, TablePath> tieringTable, Throwable throwable) {
        checkState(throwable == null, "Failed to request tiering table due to:", throwable);
        generateTieringSplits(tieringTable);
        if (!readersAwaitingSplit.isEmpty()) {
            Iterator<Integer> iterator = readersAwaitingSplit.iterator();
            while (iterator.hasNext()) {
                Integer nextAwaitingReader = iterator.next();
                if (context.registeredReaders().containsKey(nextAwaitingReader)) {
                    readersAwaitingSplit.remove(nextAwaitingReader);
                    continue;
                }
                if (!pendingSplits.isEmpty()) {
                    TieringSplit tieringSplit = pendingSplits.remove(0);
                    context.assignSplit(tieringSplit, nextAwaitingReader);
                    readersAwaitingSplit.remove(nextAwaitingReader);
                }
            }
        }
    }

    private Tuple2<Long, TablePath> requestTieringTableSplitsViaHeartBeat() {
        if (pendingSplits.isEmpty()) {
            LakeTieringHeartbeatResponse heartbeatResponse =
                    HeartBeatHelper.waitHeartbeatResponse(
                            coordinatorGateway.lakeTieringHeartbeat(
                                    HeartBeatHelper.requestTieringSplitHeartBeat()),
                            rpcRequestTimeoutMs);
            if (heartbeatResponse.hasTieringTable()) {
                if (coordinatorEpoch != heartbeatResponse.getCoordinatorEpoch()) {
                    LOGGER.warn(
                            "Fluss Coordinator epoch changed from {} to {}",
                            coordinatorEpoch,
                            heartbeatResponse.getCoordinatorEpoch());
                }
                PbLakeTieringTableInfo tieringTable = heartbeatResponse.getTieringTable();
                return Tuple2.of(
                        tieringTable.getTableId(),
                        TablePath.of(
                                tieringTable.getTablePath().getDatabaseName(),
                                tieringTable.getTablePath().getTableName()));
            } else {
                LOGGER.info("No Tiering table available.");
            }
        }
        return null;
    }

    private void generateTieringSplits(Tuple2<Long, TablePath> tieringTable) {
        if (tieringTable == null) {
            return;
        }
        LOGGER.info("Generating Tiering splits for table {}.", tieringTable.f1);
        try {
            TableInfo tableInfo = flussAdmin.getTableInfo(tieringTable.f1).get();
            if (tableInfo.hasPrimaryKey()) {
                // snapshotSplit

            } else {
                // TODO generate Tiering splits for table, how to decide the tiering offset
                // logSplit
            }
            // pendingSplits.add()
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public void addSplitsBack(List<TieringSplit> splits, int subtaskId) {
        readersAwaitingSplit.add(subtaskId);
        pendingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        LOGGER.info("Adding reader: {} to Tiering Source enumerator.", subtaskId);
        if (context.registeredReaders().containsKey(subtaskId)) {
            readersAwaitingSplit.add(subtaskId);
        }
    }

    @Override
    public TieringSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        // do nothing, the downstream lake commiter will snapshot the state to Fluss Cluster
        return new TieringSourceEnumeratorState();
    }

    @Override
    public void close() throws IOException {
        if (rpcClient != null) {
            try {
                LakeTieringHeartbeatResponse response =
                        HeartBeatHelper.waitHeartbeatResponse(
                                coordinatorGateway.lakeTieringHeartbeat(
                                        HeartBeatHelper.failedSplitHeartBeat(
                                                pendingSplits,
                                                coordinatorEpoch,
                                                tieringServiceEpoch)),
                                rpcRequestTimeoutMs);

                response.getFailedTableRespsCount();

            } catch (Exception e) {
                LOGGER.error("Failed to un-register Tiering Service from Fluss cluster.", e);
            }
            try {
                LOGGER.info(
                        "Closing Tiering Source Enumerator of epoch {} at epoch {}.",
                        tieringServiceEpoch,
                        System.currentTimeMillis());
                rpcClient.close();
            } catch (Exception e) {
                LOGGER.error("Failed to close Tiering Source enumerator.", e);
                throw new RuntimeException(e);
            }
        }
        try {
            if (flussAdmin != null) {
                LOGGER.info("Closing Fluss Admin client...");
                flussAdmin.close();
            }
        } catch (Exception e) {
            LOGGER.error("Failed to close Fluss Admin client.", e);
        }
        try {
            if (connection != null) {
                LOGGER.info("Closing Fluss connection...");
                connection.close();
            }
        } catch (Exception e) {
            LOGGER.error("Failed to close Fluss connection.", e);
        }
    }

    /** A helper class to build heartbeat request. */
    private static class HeartBeatHelper {

        public static LakeTieringHeartbeatRequest registerHeartBeat() {
            return new LakeTieringHeartbeatRequest();
        }

        public static LakeTieringHeartbeatRequest requestTieringSplitHeartBeat() {
            LakeTieringHeartbeatRequest heartbeatRequest = new LakeTieringHeartbeatRequest();
            heartbeatRequest.setRequestTable(true);
            return heartbeatRequest;
        }

        public static LakeTieringHeartbeatRequest failedSplitHeartBeat(
                List<TieringSplit> pendingSplits,
                long coordinatorEpoch,
                long tieringSererviceEpoch) {
            Set<PbHeartbeatReqForTable> failedTables = new HashSet<>();
            if (!pendingSplits.isEmpty()) {
                failedTables =
                        pendingSplits.stream()
                                .map(s -> s.getTableBucket().getTableId())
                                .collect(Collectors.toSet())
                                .stream()
                                .map(
                                        t ->
                                                new PbHeartbeatReqForTable()
                                                        .setTableId(t)
                                                        .setCoordinatorEpoch(coordinatorEpoch)
                                                        .setTieringEpoch(tieringSererviceEpoch))
                                .collect(Collectors.toSet());
            }
            return new LakeTieringHeartbeatRequest().addAllFailedTables(failedTables);
        }

        public static LakeTieringHeartbeatResponse waitHeartbeatResponse(
                CompletableFuture<LakeTieringHeartbeatResponse> responseCompletableFuture,
                long timeoutMs) {
            try {
                return responseCompletableFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                LOGGER.error("Failed to wait heartbeat response due to timeout after %s mills.", e);
                throw new FlinkRuntimeException(
                        String.format(
                                "Failed to wait heartbeat response due to timeout after %s mills.",
                                timeoutMs),
                        e);
            }
        }
    }
}
