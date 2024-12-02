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

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.utils.MathUtils;
import com.alibaba.fluss.utils.MurmurHashUtils;
import com.alibaba.fluss.utils.Preconditions;

import javax.annotation.Nullable;

import static com.alibaba.fluss.utils.UnsafeUtils.BYTE_ARRAY_BASE_OFFSET;

/** Hash bucket assigner. */
@Internal
public class HashBucketAssigner implements BucketAssigner {

    private final Integer numBuckets;

    public HashBucketAssigner(Integer numBuckets) {
        Preconditions.checkNotNull(
                numBuckets, "Number of buckets must not be null for hash bucket assigner !");
        this.numBuckets = numBuckets;
    }

    @Override
    public int assignBucket(@Nullable byte[] key, Cluster cluster) {
        Preconditions.checkArgument(key != null, "Key must not be null for hash bucket assigner !");
        return bucketForRowKey(key, this.numBuckets);
    }

    @Override
    public boolean abortIfBatchFull() {
        return false;
    }

    @Override
    public void close() {
        // do nothing now.
    }

    /**
     * If the table contains primary key, the default hashing function to choose a bucket from the
     * serialized key bytes.
     */
    public static int bucketForRowKey(final byte[] key, final int numBuckets) {
        Preconditions.checkArgument(key.length != 0, "Assigned key must not be empty!");
        int keyHash = MurmurHashUtils.hashUnsafeBytes(key, BYTE_ARRAY_BASE_OFFSET, key.length);
        return MathUtils.murmurHash(keyHash) % numBuckets;
    }
}
