/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.laketiering;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.source.split.HybridSnapshotLogSplit;
import com.alibaba.fluss.flink.source.split.HybridSnapshotLogSplitState;
import com.alibaba.fluss.flink.source.split.LogSplit;
import com.alibaba.fluss.flink.source.split.LogSplitState;
import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.flink.source.split.SourceSplitState;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;

import java.util.Map;

/** A {@link SourceReader} that read records from Fluss and write to lake. */
@Internal
public final class LakeTieringSourceReader<WriteResult>
        extends SingleThreadMultiplexSourceReaderBase<
                TableBucketWriteResult<WriteResult>,
                TableBucketWriteResult<WriteResult>,
                SourceSplitBase,
                SourceSplitState> {

    public LakeTieringSourceReader(
            SourceReaderContext context,
            Configuration flussConf,
            LakeTieringFactory<WriteResult, ?> lakeTieringFactory) {
        super(
                () -> new LakeTieringSplitReader<>(flussConf, lakeTieringFactory),
                new TableBucketWriteResultEmitter<>(),
                context.getConfiguration(),
                context);
    }

    @Override
    public void start() {
        // we request a split only if we did not get splits during the checkpoint restore
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, SourceSplitState> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    protected SourceSplitState initializedState(SourceSplitBase split) {
        if (split instanceof HybridSnapshotLogSplit) {
            return new HybridSnapshotLogSplitState((HybridSnapshotLogSplit) split);
        } else if (split instanceof LogSplit) {
            return new LogSplitState(split.asLogSplit());
        } else {
            throw new UnsupportedOperationException("Unsupported split type: " + split);
        }
    }

    @Override
    protected SourceSplitBase toSplitType(String splitId, SourceSplitState splitState) {
        return splitState.toSourceSplit();
    }
}
