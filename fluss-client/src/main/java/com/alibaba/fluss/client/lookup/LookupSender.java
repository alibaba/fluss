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

package com.alibaba.fluss.client.lookup;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.IndexLookupRequest;
import com.alibaba.fluss.rpc.messages.IndexLookupResponse;
import com.alibaba.fluss.rpc.messages.LookupRequest;
import com.alibaba.fluss.rpc.messages.LookupResponse;
import com.alibaba.fluss.rpc.messages.PbIndexLookupRespForBucket;
import com.alibaba.fluss.rpc.messages.PbIndexLookupRespForKey;
import com.alibaba.fluss.rpc.messages.PbLookupRespForBucket;
import com.alibaba.fluss.rpc.protocol.ApiError;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import static com.alibaba.fluss.client.utils.ClientRpcMessageUtils.makeIndexLookupRequest;
import static com.alibaba.fluss.client.utils.ClientRpcMessageUtils.makeLookupRequest;

/**
 * This background thread pool lookup operations from {@link #lookupQueue}, and send lookup requests
 * to the tablet server.
 */
@Internal
class LookupSender implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(LookupSender.class);

    private volatile boolean running;

    /** true when the caller wants to ignore all unsent/inflight messages and force close. */
    private volatile boolean forceClose;

    private final MetadataUpdater metadataUpdater;

    private final LookupQueue lookupQueue;

    private final Semaphore maxInFlightReuqestsSemaphore;

    LookupSender(MetadataUpdater metadataUpdater, LookupQueue lookupQueue, int maxFlightRequests) {
        this.metadataUpdater = metadataUpdater;
        this.lookupQueue = lookupQueue;
        this.maxInFlightReuqestsSemaphore = new Semaphore(maxFlightRequests);
        this.running = true;
    }

    @Override
    public void run() {
        LOG.debug("Starting Fluss lookup sender thread.");

        // main loop, runs until close is called.
        while (running) {
            try {
                runOnce(false);
            } catch (Throwable t) {
                LOG.error("Uncaught error in Fluss lookup sender thread: ", t);
            }
        }

        LOG.debug("Beginning shutdown of Fluss lookup I/O thread, sending remaining records.");

        // okay we stopped accepting requests but there may still be requests in the accumulator or
        // waiting for acknowledgment, wait until these are completed.
        // TODO Check the in flight request count in the accumulator.
        if (!forceClose && lookupQueue.hasUnDrained()) {
            try {
                runOnce(true);
            } catch (Exception e) {
                LOG.error("Uncaught error in Fluss lookup sender thread: ", e);
            }
        }

        // TODO if force close failed, add logic to abort incomplete lookup requests.
        LOG.debug("Shutdown of Fluss lookup sender I/O thread has completed.");
    }

    /** Run a single iteration of sending. */
    private void runOnce(boolean drainAll) throws Exception {
        if (drainAll) {
            sendLookups(LookupType.LOOKUP, lookupQueue.drainAll(LookupType.LOOKUP));
            sendLookups(LookupType.INDEX_LOOKUP, lookupQueue.drainAll(LookupType.INDEX_LOOKUP));
        } else {
            LookupType lookupType = lookupQueue.nexDrainLookupType();
            sendLookups(lookupType, lookupQueue.drain(lookupType));
        }
    }

    private void sendLookups(LookupType lookupType, List<AbstractLookup> lookups) {
        if (lookups.isEmpty()) {
            return;
        }
        // group by <leader, lookup batches> to lookup batches
        Map<Integer, List<AbstractLookup>> lookupBatches = groupByLeader(lookups);
        // now, send the batches
        lookupBatches.forEach((destination, batch) -> sendLookups(destination, lookupType, batch));
    }

    private Map<Integer, List<AbstractLookup>> groupByLeader(List<AbstractLookup> lookups) {
        // leader -> lookup batches
        Map<Integer, List<AbstractLookup>> lookupBatchesByLeader = new HashMap<>();
        for (AbstractLookup abstractLookup : lookups) {
            int leader;
            if (abstractLookup instanceof Lookup) {
                Lookup lookup = (Lookup) abstractLookup;
                // lookup the leader node
                TableBucket tb = lookup.tableBucket();
                leader = metadataUpdater.leaderFor(tb);
            } else if (abstractLookup instanceof IndexLookup) {
                IndexLookup indexLookup = (IndexLookup) abstractLookup;
                leader = indexLookup.destination();
            } else {
                throw new IllegalArgumentException("Unsupported lookup type: " + abstractLookup);
            }
            lookupBatchesByLeader
                    .computeIfAbsent(leader, k -> new ArrayList<>())
                    .add(abstractLookup);
        }
        return lookupBatchesByLeader;
    }

    private void sendLookups(
            int destination, LookupType lookupType, List<AbstractLookup> lookupBatches) {
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(destination);

        if (lookupType == LookupType.LOOKUP) {
            sendLookupRequest(gateway, lookupBatches);
        } else if (lookupType == LookupType.INDEX_LOOKUP) {
            sendIndexLookupRequest(gateway, lookupBatches);
        } else {
            throw new IllegalArgumentException("Unsupported lookup type: " + lookupType);
        }
    }

    private void sendLookupRequest(
            TabletServerGateway gateway, List<AbstractLookup> lookupBatches) {
        // table id -> (bucket -> lookups)
        Map<Long, Map<TableBucket, LookupBatch>> lookupByTableId = new HashMap<>();
        for (AbstractLookup abstractLookup : lookupBatches) {
            Lookup lookup = (Lookup) abstractLookup;
            TableBucket tb = lookup.tableBucket();
            long tableId = tb.getTableId();
            lookupByTableId
                    .computeIfAbsent(tableId, k -> new HashMap<>())
                    .computeIfAbsent(tb, k -> new LookupBatch(tb))
                    .addLookup(lookup);
        }

        lookupByTableId.forEach(
                (tableId, lookupsByBucket) ->
                        sendLookupRequestAndHandleResponse(
                                gateway,
                                makeLookupRequest(tableId, lookupsByBucket.values()),
                                tableId,
                                lookupsByBucket));
    }

    private void sendIndexLookupRequest(
            TabletServerGateway gateway, List<AbstractLookup> indexLookupBatches) {
        // table id -> (bucket -> lookups)
        Map<Long, Map<TableBucket, IndexLookupBatch>> lookupByTableId = new HashMap<>();
        for (AbstractLookup abstractLookup : indexLookupBatches) {
            IndexLookup indexLookup = (IndexLookup) abstractLookup;
            TableBucket tb = new TableBucket(indexLookup.tableId(), indexLookup.bucketId());
            long tableId = tb.getTableId();
            lookupByTableId
                    .computeIfAbsent(tableId, k -> new HashMap<>())
                    .computeIfAbsent(tb, k -> new IndexLookupBatch(tb))
                    .addLookup(indexLookup);
        }

        lookupByTableId.forEach(
                (tableId, indexLookupBatch) ->
                        sendIndexLookupRequestAndHandleResponse(
                                gateway,
                                makeIndexLookupRequest(tableId, indexLookupBatch.values()),
                                tableId,
                                indexLookupBatch));
    }

    private void sendLookupRequestAndHandleResponse(
            TabletServerGateway gateway,
            LookupRequest lookupRequest,
            long tableId,
            Map<TableBucket, LookupBatch> lookupsByBucket) {
        try {
            maxInFlightReuqestsSemaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlussRuntimeException("interrupted:", e);
        }
        gateway.lookup(lookupRequest)
                .thenAccept(
                        lookupResponse -> {
                            try {
                                handleLookupResponse(tableId, lookupResponse, lookupsByBucket);
                            } finally {
                                maxInFlightReuqestsSemaphore.release();
                            }
                        })
                .exceptionally(
                        e -> {
                            try {
                                handleLookupRequestException(e, lookupsByBucket);
                                return null;
                            } finally {
                                maxInFlightReuqestsSemaphore.release();
                            }
                        });
    }

    private void sendIndexLookupRequestAndHandleResponse(
            TabletServerGateway gateway,
            IndexLookupRequest indexLookupRequest,
            long tableId,
            Map<TableBucket, IndexLookupBatch> lookupsByBucket) {
        try {
            maxInFlightReuqestsSemaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlussRuntimeException("interrupted:", e);
        }
        gateway.indexLookup(indexLookupRequest)
                .thenAccept(
                        indexLookupResponse -> {
                            try {
                                handleIndexLookupResponse(
                                        tableId, indexLookupResponse, lookupsByBucket);
                            } finally {
                                maxInFlightReuqestsSemaphore.release();
                            }
                        })
                .exceptionally(
                        e -> {
                            try {
                                handleIndexLookupException(e, lookupsByBucket);
                                return null;
                            } finally {
                                maxInFlightReuqestsSemaphore.release();
                            }
                        });
    }

    private void handleLookupResponse(
            long tableId,
            LookupResponse lookupResponse,
            Map<TableBucket, LookupBatch> lookupsByBucket) {
        for (PbLookupRespForBucket pbLookupRespForBucket : lookupResponse.getBucketsRespsList()) {
            TableBucket tableBucket =
                    new TableBucket(
                            tableId,
                            pbLookupRespForBucket.hasPartitionId()
                                    ? pbLookupRespForBucket.getPartitionId()
                                    : null,
                            pbLookupRespForBucket.getBucketId());

            // TODO If error, we need to retry send the request instead of throw exception.
            LookupBatch lookupBatch = lookupsByBucket.get(tableBucket);
            if (pbLookupRespForBucket.hasErrorCode()) {
                LOG.warn(
                        "Get error lookup response on table bucket {}, fail. Error: {}",
                        tableBucket,
                        pbLookupRespForBucket.getErrorMessage());
                lookupBatch.completeExceptionally(
                        ApiError.fromErrorMessage(pbLookupRespForBucket).exception());
            } else {
                List<List<byte[]>> byteValues =
                        pbLookupRespForBucket.getValuesList().stream()
                                .map(
                                        pbValue -> {
                                            if (pbValue.hasValues()) {
                                                return Collections.singletonList(
                                                        pbValue.getValues());
                                            } else {
                                                return null;
                                            }
                                        })
                                .collect(Collectors.toList());
                lookupBatch.complete(byteValues);
            }
        }
    }

    private void handleIndexLookupResponse(
            long tableId,
            IndexLookupResponse indexLookupResponse,
            Map<TableBucket, IndexLookupBatch> indexLookupsByBucket) {
        for (PbIndexLookupRespForBucket respForBucket : indexLookupResponse.getBucketsRespsList()) {
            TableBucket tableBucket =
                    new TableBucket(
                            tableId,
                            respForBucket.hasPartitionId() ? respForBucket.getPartitionId() : null,
                            respForBucket.getBucketId());

            // TODO If error, we need to retry send the request instead of throw exception.
            IndexLookupBatch indexLookupBatch = indexLookupsByBucket.get(tableBucket);
            List<List<byte[]>> result = new ArrayList<>();
            for (int i = 0; i < respForBucket.getKeysRespsCount(); i++) {
                PbIndexLookupRespForKey respForKey = respForBucket.getKeysRespAt(i);
                List<byte[]> keyResult = new ArrayList<>();
                for (int j = 0; j < respForKey.getValuesCount(); j++) {
                    keyResult.add(respForKey.getValueAt(j));
                }
                result.add(keyResult);
            }
            indexLookupBatch.complete(result);
        }
    }

    private void handleLookupRequestException(
            Throwable t, Map<TableBucket, LookupBatch> lookupsByBucket) {
        ApiError error = ApiError.fromThrowable(t);
        for (LookupBatch lookupBatch : lookupsByBucket.values()) {
            LOG.warn(
                    "Get error lookup response on table bucket {}, fail. Error: {}",
                    lookupBatch.tableBucket(),
                    error.formatErrMsg());
            lookupBatch.completeExceptionally(error.exception());
        }
    }

    private void handleIndexLookupException(
            Throwable t, Map<TableBucket, IndexLookupBatch> lookupsByBucket) {
        ApiError error = ApiError.fromThrowable(t);
        for (IndexLookupBatch lookupBatch : lookupsByBucket.values()) {
            // TODO If error, we need to retry send the request instead of throw exception.
            LOG.warn(
                    "Get error index lookup response on table bucket {}, fail. Error: {}",
                    lookupBatch.tableBucket(),
                    error.formatErrMsg());
            lookupBatch.completeExceptionally(error.exception());
        }
    }

    void forceClose() {
        forceClose = true;
        initiateClose();
    }

    void initiateClose() {
        // Ensure accumulator is closed first to guarantee that no more appends are accepted after
        // breaking from the sender loop. Otherwise, we may miss some callbacks when shutting down.
        lookupQueue.close();
        running = false;
    }
}
