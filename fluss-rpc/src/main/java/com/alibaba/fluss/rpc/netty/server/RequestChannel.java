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

package com.alibaba.fluss.rpc.netty.server;

import com.alibaba.fluss.rpc.messages.FetchLogRequest;
import com.alibaba.fluss.rpc.protocol.ApiKeys;
import com.alibaba.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/** A blocking queue channel that can receive requests and send responses. */
@ThreadSafe
public final class RequestChannel {
    private static final Logger LOG = LoggerFactory.getLogger(RequestChannel.class);

    private final BlockingQueue<RpcRequest> requestQueue;

    /**
     * This queue is used to handle replica synchronization requests ({@link FetchLogRequest} from
     * follower) between different machines in the Fluss cluster. The reason for using a separate
     * queue is to prevent client read/write requests from affecting replica synchronization, which
     * could otherwise impact the stability of the cluster.
     */
    private final BlockingQueue<RpcRequest> replicaSyncQueue;

    private boolean pollFromReplicaSyncQueue;

    public RequestChannel(int queueCapacity) {
        int eachQueueCapacity = queueCapacity / 2;
        this.requestQueue = new ArrayBlockingQueue<>(eachQueueCapacity);
        this.replicaSyncQueue = new ArrayBlockingQueue<>(eachQueueCapacity);
        this.pollFromReplicaSyncQueue = true;
    }

    /**
     * Send a request to be handled, potentially blocking until there is room in the queue for the
     * request.
     */
    public void putRequest(RpcRequest request) throws Exception {
        if (request.getApiKey() == ApiKeys.FETCH_LOG.id
                && ((FetchLogRequest) request.getMessage()).getFollowerServerId() >= 0) {
            replicaSyncQueue.put(request);
        } else {
            requestQueue.put(request);
        }
    }

    /**
     * Sends a shutdown request to the channel. This can allow request processor gracefully
     * shutdown.
     */
    public void putShutdownRequest() throws Exception {
        putRequest(RpcRequest.SHUTDOWN_REQUEST);
    }

    /**
     * Get the next request or block until specified time has elapsed.
     *
     * @return the head of this queue, or null if the specified waiting time elapses before an
     *     element is available.
     */
    public RpcRequest pollRequest(long timeoutMs) {
        long startTime = System.nanoTime();
        long remainingTime = timeoutMs;
        Tuple2<BlockingQueue<RpcRequest>, BlockingQueue<RpcRequest>> queuePair =
                pollFromReplicaSyncQueue
                        ? Tuple2.of(replicaSyncQueue, requestQueue)
                        : Tuple2.of(requestQueue, replicaSyncQueue);
        pollFromReplicaSyncQueue = !pollFromReplicaSyncQueue;
        try {
            while (remainingTime > 0) {
                RpcRequest request = queuePair.f0.poll(1, TimeUnit.MILLISECONDS);
                if (request != null) {
                    return request;
                }

                request = queuePair.f1.poll(1, TimeUnit.MILLISECONDS);
                if (request != null) {
                    return request;
                }

                remainingTime = timeoutMs - (System.nanoTime() - startTime);
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while polling requests from channel queue.", e);
            return null;
        }
        return null;
    }

    /** Get the number of requests in the queue. */
    public int requestsCount() {
        return requestQueue.size() + replicaSyncQueue.size();
    }
}
