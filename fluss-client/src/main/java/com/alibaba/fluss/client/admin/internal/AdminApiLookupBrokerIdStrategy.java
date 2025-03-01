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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * The lookup strategy to lookup the broker IDs for the given keys. For example, lookup bucketId's
 * brokerId for listOffset.
 *
 * @param <K>
 */
public interface AdminApiLookupBrokerIdStrategy<K> {

    /**
     * Lookup the broker IDs for the given keys. The handler should parse the response, check for
     * errors, and return a result indicating which keys were mapped to a brokerId successfully and
     * which keys received a fatal error. Note that keys which receive a retriable error should be
     * left out of the result. They will be retried automatically. For example, if the response of
     * `ListOffset` request indicates no leader is ready, then the key should be left out of the
     * result so that the request will be retried.
     *
     * @param keys the set of keys that require lookup
     * @return a result indicating which keys mapped successfully to a brokerId and which
     *     encountered a fatal error
     */
    CompletableFuture<LookupBrokerIdResult<K>> lookup(Set<K> keys);

    class LookupBrokerIdResult<K> {

        // This is the set of keys that have been mapped to a specific broker for
        // fulfillment of the API request.
        public final Map<K, Integer> mappedKeys;

        // This is the set of keys that have encountered a fatal error during the lookup
        // phase. The driver will not attempt lookup or fulfillment for failed keys.
        public final Map<K, Throwable> failedKeys;

        public LookupBrokerIdResult(Map<K, Integer> mappedKeys, Map<K, Throwable> failedKeys) {
            this.mappedKeys = Collections.unmodifiableMap(mappedKeys);
            this.failedKeys = Collections.unmodifiableMap(failedKeys);
        }
    }
}
