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

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Class to represent an index lookup operation, it contains the table id, bucketNums and related
 * CompletableFuture.
 */
@Internal
public class IndexLookup extends AbstractLookup {
    private final int destination;
    private final long tableId;
    private final int bucketId;
    private final String indexName;
    private final CompletableFuture<List<byte[]>> future;

    public IndexLookup(
            int destination, long tableId, int bucketId, String indexName, byte[] indexKey) {
        super(indexKey);
        this.destination = destination;
        this.tableId = tableId;
        this.bucketId = bucketId;
        this.indexName = indexName;
        this.future = new CompletableFuture<>();
    }

    public int destination() {
        return destination;
    }

    public long tableId() {
        return tableId;
    }

    public int bucketId() {
        return bucketId;
    }

    public String indexName() {
        return indexName;
    }

    public CompletableFuture<List<byte[]>> future() {
        return future;
    }

    @Override
    public LookupType lookupType() {
        return LookupType.INDEX_LOOKUP;
    }
}
