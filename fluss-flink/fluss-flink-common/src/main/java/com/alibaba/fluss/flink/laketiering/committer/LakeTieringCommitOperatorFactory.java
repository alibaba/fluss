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

package com.alibaba.fluss.flink.laketiering.committer;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.laketiering.TableBucketWriteResult;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.source.coordinator.SourceCoordinatorProvider;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

/** The factory to create {@link LakeTieringCommitOperator}. */
public class LakeTieringCommitOperatorFactory<WriteResult, Committable>
        extends AbstractStreamOperatorFactory<Committable>
        implements CoordinatedOperatorFactory<Committable> {

    /** The number of worker thread for the source coordinator. */
    private final int numCoordinatorWorkerThread;

    private final Configuration flussConfig;
    private final Source<TableBucketWriteResult<WriteResult>, ?, ?> source;
    private final WatermarkStrategy<TableBucketWriteResult<WriteResult>> watermarkStrategy;
    private final LakeTieringFactory<WriteResult, Committable> lakeTieringFactory;

    public LakeTieringCommitOperatorFactory(
            Configuration flussConfig,
            Source<TableBucketWriteResult<WriteResult>, ?, ?> source,
            WatermarkStrategy<TableBucketWriteResult<WriteResult>> watermarkStrategy,
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory) {
        this.flussConfig = flussConfig;
        this.source = source;
        this.watermarkStrategy = watermarkStrategy;
        this.lakeTieringFactory = lakeTieringFactory;
        this.numCoordinatorWorkerThread = 1;
    }

    @Override
    public <T extends StreamOperator<Committable>> T createStreamOperator(
            StreamOperatorParameters<Committable> parameters) {
        final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
        final OperatorEventGateway gateway =
                parameters.getOperatorEventDispatcher().getOperatorEventGateway(operatorId);

        LakeTieringCommitOperator<WriteResult, Committable> commitOperator =
                new LakeTieringCommitOperator<>(
                        parameters, flussConfig, gateway, lakeTieringFactory);
        parameters.getOperatorEventDispatcher().registerEventHandler(operatorId, commitOperator);

        @SuppressWarnings("unchecked")
        final T castedOperator = (T) commitOperator;

        return castedOperator;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        return new SourceCoordinatorProvider<>(
                operatorName,
                operatorID,
                source,
                numCoordinatorWorkerThread,
                watermarkStrategy.getAlignmentParameters(),
                null);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return LakeTieringCommitOperator.class;
    }
}
