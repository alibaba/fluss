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

package com.alibaba.fluss.connector.flink.source.split;

import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.fs.FsPathAndFileName;
import com.alibaba.fluss.metadata.TableBucket;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link com.alibaba.fluss.connector.flink.source.split.SourceSplitSerializer} of
 * serializing {@link com.alibaba.fluss.connector.flink.source.split.SnapshotSplit} and {@link
 * com.alibaba.fluss.connector.flink.source.split.LogSplit}.
 */
class SourceSplitSerializerTest {

    private static final SourceSplitSerializer serializer = SourceSplitSerializer.INSTANCE;
    private static final TableBucket tableBucket = new TableBucket(1, 2);
    private static final TableBucket partitionedTableBucket = new TableBucket(1, 100L, 2);

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testHybridSnapshotLogSplitSerde(boolean isPartitioned) throws Exception {
        int recordsToSkip = 3;
        List<FsPathAndFileName> snapshotFiles =
                Arrays.asList(
                        new FsPathAndFileName(new FsPath("oss://bucket/path/f1"), "fname1"),
                        new FsPathAndFileName(new FsPath("oss://bucket/path/f2"), "fname2"));

        TableBucket bucket = isPartitioned ? partitionedTableBucket : tableBucket;
        String partitionName = isPartitioned ? "2024" : null;

        HybridSnapshotLogSplit split =
                new HybridSnapshotLogSplit(bucket, partitionName, snapshotFiles, recordsToSkip);
        byte[] serialized = serializer.serialize(split);

        SourceSplitBase deserializedSplit =
                serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(split);

        split =
                new HybridSnapshotLogSplit(
                        bucket, partitionName, snapshotFiles, recordsToSkip, true, 5, 2);
        serialized = serializer.serialize(split);
        deserializedSplit = serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(split);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testLogSplitSerde(boolean isPartitioned) throws Exception {
        TableBucket bucket = isPartitioned ? partitionedTableBucket : tableBucket;
        String partitionName = isPartitioned ? "2024" : null;
        LogSplit logSplit = new LogSplit(bucket, partitionName, 100);

        byte[] serialized = serializer.serialize(logSplit);
        SourceSplitBase deserializedSplit =
                serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(logSplit);
    }
}
