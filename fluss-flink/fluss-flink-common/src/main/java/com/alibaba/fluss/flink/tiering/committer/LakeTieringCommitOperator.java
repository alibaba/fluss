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

package com.alibaba.fluss.flink.tiering.committer;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.tiering.event.FinishTieringEvent;
import com.alibaba.fluss.flink.tiering.source.TableBucketWriteResult;
import com.alibaba.fluss.lakehouse.committer.LakeCommitter;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;
import com.alibaba.fluss.metadata.TableBucket;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.Preconditions.checkState;

/**
 * A Flink operator to aggregate {@link WriteResult}s by table to {@link Committable} which will
 * then be committed to lake & Fluss cluster.
 */
public class LakeTieringCommitOperator<WriteResult, Committable>
        extends AbstractStreamOperator<Committable>
        implements OneInputStreamOperator<TableBucketWriteResult<WriteResult>, Committable>,
                OperatorEventHandler {

    private static final long serialVersionUID = 1L;

    private final LakeTieringFactory<WriteResult, Committable> lakeTieringFactory;
    private final OperatorEventGateway operatorEventGateway;
    private final TableLakeSnapshotCommitter tableLakeSnapshotCommitter;

    // tableid -> write results
    private final Map<Long, List<TableBucketWriteResult<WriteResult>>>
            collectedTableBucketWriteResults;

    public LakeTieringCommitOperator(
            StreamOperatorParameters<Committable> parameters,
            Configuration flussConf,
            OperatorEventGateway operatorEventGateway,
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory) {
        this.lakeTieringFactory = lakeTieringFactory;
        this.operatorEventGateway = operatorEventGateway;
        this.tableLakeSnapshotCommitter = new TableLakeSnapshotCommitter(flussConf);
        this.collectedTableBucketWriteResults = new HashMap<>();
        this.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
    }

    @Override
    public void open() {
        tableLakeSnapshotCommitter.open();
    }

    @Override
    public void processElement(StreamRecord<TableBucketWriteResult<WriteResult>> streamRecord)
            throws Exception {
        TableBucketWriteResult<WriteResult> tableBucketWriteResult = streamRecord.getValue();
        TableBucket tableBucket = tableBucketWriteResult.tableBucket();
        long tableId = tableBucket.getTableId();
        registerTableBucketWriteResult(tableId, tableBucketWriteResult);

        // may collect all write results for the table
        List<TableBucketWriteResult<WriteResult>> committableWriteResults =
                collectTableAllBucketWriteResult(tableId);

        if (committableWriteResults != null) {
            commitWriteResults(tableId, committableWriteResults);
            collectedTableBucketWriteResults.remove(tableId);
            // notify that the table id has been finished tier
            operatorEventGateway.sendEventToCoordinator(
                    new SourceEventWrapper(new FinishTieringEvent(tableId)));
        }
    }

    private void commitWriteResults(
            long tableId, List<TableBucketWriteResult<WriteResult>> committableWriteResults)
            throws Exception {
        try (LakeCommitter<WriteResult, Committable> lakeCommitter =
                lakeTieringFactory.createLakeCommitter()) {
            List<WriteResult> writeResults =
                    committableWriteResults.stream()
                            .map(TableBucketWriteResult::writeResult)
                            .collect(Collectors.toList());
            // to committable
            Committable committable = lakeCommitter.toCommitable(writeResults);
            long commitedSnapshotId = lakeCommitter.commit(committable);
            // commit to fluss
            Map<TableBucket, Long> logEndOffsets = new HashMap<>();
            for (TableBucketWriteResult<WriteResult> writeResult : committableWriteResults) {
                logEndOffsets.put(writeResult.tableBucket(), writeResult.logEndOffset());
            }
            tableLakeSnapshotCommitter.commit(
                    new TableLakeSnapshot(tableId, commitedSnapshotId, logEndOffsets));
        }
    }

    private void registerTableBucketWriteResult(
            long tableId, TableBucketWriteResult<WriteResult> tableBucketWriteResult) {
        collectedTableBucketWriteResults
                .computeIfAbsent(tableId, k -> new ArrayList<>())
                .add(tableBucketWriteResult);
    }

    @Nullable
    private List<TableBucketWriteResult<WriteResult>> collectTableAllBucketWriteResult(
            long tableId) {
        Set<TableBucket> collectedBuckets = new HashSet<>();
        Integer numberOfWriteResults = null;
        List<TableBucketWriteResult<WriteResult>> writeResults = new ArrayList<>();
        for (TableBucketWriteResult<WriteResult> tableBucketWriteResult :
                collectedTableBucketWriteResults.get(tableId)) {
            if (!collectedBuckets.add(tableBucketWriteResult.tableBucket())) {
                // it means the write results contain more than two write result
                // for same table, it shouldn't happen, let's throw exception to
                // avoid unexpected behavior
                throw new IllegalStateException(
                        String.format(
                                "Found duplicate write results for bucket %s of table %s.",
                                tableBucketWriteResult.tableBucket(), tableId));
            }
            if (numberOfWriteResults == null) {
                numberOfWriteResults = tableBucketWriteResult.numberOfWriteResults();
            } else {
                // the numberOfWriteResults must be same across tableBucketWriteResults
                checkState(
                        numberOfWriteResults == tableBucketWriteResult.numberOfWriteResults(),
                        "numberOfWriteResults is not same across TableBucketWriteResults for table %s, got %s and %s.",
                        tableId,
                        numberOfWriteResults,
                        tableBucketWriteResult.numberOfWriteResults());
            }
            writeResults.add(tableBucketWriteResult);
        }

        if (numberOfWriteResults != null && writeResults.size() == numberOfWriteResults) {
            return writeResults;
        } else {
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        tableLakeSnapshotCommitter.close();
    }

    @Override
    public void handleOperatorEvent(OperatorEvent operatorEvent) {
        // do-nothing
    }
}
