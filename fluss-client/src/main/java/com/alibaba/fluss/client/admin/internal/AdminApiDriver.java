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

package com.alibaba.fluss.client.admin.internal;

import com.alibaba.fluss.annotation.Internal;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A driver use for multi-stage request workflows such as we see with the list offsets APIs or any
 * request which needs to be sent to a bucket leader. Typically these APIs have two concrete stages:
 * 1. Lookup: Find the broker that can fulfill the request (e. g. bucket leader or coordinator) 2.
 * Fulfillment: Send the request to the broker found in the first step This is complicated by the
 * fact that `Admin` APIs are typically batched, which means the Lookup stage may result in a set of
 * brokers. For example, take a `ListOffsets` request for a set of table buckets . In the Lookup
 * stage, we will find the bucket leaders for this set of buckets; in the Fulfillment stage, we will
 * group together bucket according to the IDs of the discovered leaders. Additionally, the flow
 * between these two stages is bi-directional. We may find after sending a `ListOffsets` request to
 * an expected leader that there was a leader change. This would result in a table bucket being sent
 * back to the Lookup stage.
 *
 * @param <K> – The key type, which is also the granularity of the request routing (e. g. this could
 *     be `TopicPartition` in the case of requests intended for a partition leader or the `GroupId`
 *     in the case of consumer group requests intended for the group coordinator)
 * @param <V> – The fulfillment type for each key (e. g. this could be consumer group state when the
 *     key type is a consumer `GroupId`)
 */
@Internal
public class AdminApiDriver<K, V> {
    private static final long RETRY_DELAY_MS = 100L;

    private final Set<K> lookupKeys;
    private final BiMultimap<Integer, K> fulfillmentMap;
    private final AdminApiFuture<K, V> future;
    private final AdminApiHandler<K, V> handler;

    public AdminApiDriver(AdminApiFuture<K, V> future, AdminApiHandler<K, V> handler) {
        this.future = future;
        this.handler = handler;
        this.lookupKeys = new HashSet<>();
        this.fulfillmentMap = new BiMultimap<>();
        retryLookup(future.lookupKeys());
    }

    public void maybeSendRequests() {
        if (!lookupKeys.isEmpty()) {
            handler.lookupStrategy()
                    .lookup(Collections.unmodifiableSet(lookupKeys))
                    .thenAcceptAsync(
                            lookupResult -> {
                                completeLookup(lookupResult.mappedKeys);
                                completeLookupExceptionally(lookupResult.failedKeys);
                                try {
                                    Thread.sleep(RETRY_DELAY_MS);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                // maybe send requests again if not finished.
                                maybeSendRequests();
                            });
        }

        for (Map.Entry<Integer, Set<K>> entry : fulfillmentMap.entrySet()) {
            handler.handle(entry.getKey(), entry.getValue())
                    .thenAcceptAsync(
                            apiResult -> {
                                complete(apiResult.completedKeys);
                                completeExceptionally(apiResult.failedKeys);
                                retryLookup(apiResult.unmappedKeys);
                                // maybe send requests again if not finished.
                                maybeSendRequests();
                            });
        }
    }

    private void retryLookup(Collection<K> keys) {
        keys.forEach(this::unmap);
    }

    private void completeLookup(Map<K, Integer> brokerIdMapping) {
        if (!brokerIdMapping.isEmpty()) {
            future.completeLookup(brokerIdMapping);
            brokerIdMapping.forEach(this::map);
        }
    }

    private void completeLookupExceptionally(Map<K, Throwable> errors) {
        if (!errors.isEmpty()) {
            future.completeLookupExceptionally(errors);
            clear(errors.keySet());
        }
    }

    /**
     * Complete the future associated with the given key. After this is called, all keys will be
     * taken out of both the Lookup and Fulfillment stages so that request are not retried.
     */
    private void complete(Map<K, V> values) {
        if (!values.isEmpty()) {
            future.complete(values);
            clear(values.keySet());
        }
    }

    /**
     * Complete the future associated with the given key exceptionally. After is called, the key
     * will be taken out of both the Lookup and Fulfillment stages so that request are not retried.
     */
    private void completeExceptionally(Map<K, Throwable> errors) {
        if (!errors.isEmpty()) {
            future.completeExceptionally(errors);
            clear(errors.keySet());
        }
    }

    /**
     * Associate a key with a brokerId. This is called after a response in the Lookup stage reveals
     * the mapping (e.g. when the `FindCoordinator` tells us the group coordinator for a specific
     * consumer group).
     */
    private void map(K lookupKey, Integer brokerId) {
        lookupKeys.remove(lookupKey);
        fulfillmentMap.put(brokerId, lookupKey);
    }

    /**
     * Disassociate a key from the currently mapped brokerId. This will send the key back to the
     * Lookup stage, which will allow us to attempt lookup again.
     */
    private void unmap(K lookupKey) {
        fulfillmentMap.remove(lookupKey);
        lookupKeys.add(lookupKey);
    }

    private void clear(Collection<K> keys) {
        keys.forEach(
                key -> {
                    lookupKeys.remove(key);
                    fulfillmentMap.remove(key);
                });
    }

    private static class BiMultimap<K, V> {
        private final Map<V, K> reverseMap = new HashMap<>();
        private final Map<K, Set<V>> map = new HashMap<>();

        void put(K key, V value) {
            remove(value);
            reverseMap.put(value, key);
            map.computeIfAbsent(key, k -> new HashSet<>()).add(value);
        }

        void remove(V value) {
            K key = reverseMap.remove(value);
            if (key != null) {
                Set<V> set = map.get(key);
                if (set != null) {
                    set.remove(value);
                    if (set.isEmpty()) {
                        map.remove(key);
                    }
                }
            }
        }

        Set<Map.Entry<K, Set<V>>> entrySet() {
            return Collections.unmodifiableMap(map).entrySet();
        }
    }
}
