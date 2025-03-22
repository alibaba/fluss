/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.flink.source;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.serdes.FlussDeserializationSchema;
import com.alibaba.fluss.flink.source.enumerator.FlinkSourceEnumerator;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import com.alibaba.fluss.flink.source.reader.FlinkSourceReader;
import com.alibaba.fluss.flink.source.reader.RecordAndPos;
import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.flink.source.split.SourceSplitSerializer;
import com.alibaba.fluss.flink.source.state.FlussSourceEnumeratorStateSerializer;
import com.alibaba.fluss.flink.source.state.SourceEnumeratorState;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.ExecutionException;

/** Flink source for Fluss. */
public class FlussSource<OUT>
        implements Source<OUT, SourceSplitBase, SourceEnumeratorState>, ResultTypeQueryable<OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(FlussSource.class);

    private static final long serialVersionUID = 1L;

    private final Configuration flussConf;
    private final TablePath tablePath;
    private final boolean hasPrimaryKey;
    private final boolean isPartitioned;
    private final RowType sourceOutputType;
    private final int[] projectedFields;
    private final long scanPartitionDiscoveryIntervalMs;
    private final boolean streaming;

    private String bootstrapServers;
    private OffsetsInitializer offsetsInitializer;
    private FlussDeserializationSchema<OUT> deserializationSchema;

    private FlussSource(Builder<OUT> builder) {
        this.bootstrapServers = builder.bootstrapServers;
        this.offsetsInitializer = builder.offsetsInitializer;
        this.scanPartitionDiscoveryIntervalMs = builder.scanPartitionDiscoveryIntervalMs;
        this.deserializationSchema = builder.deserializationSchema;
        this.projectedFields = builder.projectedFields;
        this.streaming = builder.isStreaming;

        if (builder.flussConfig == null) {
            this.flussConf = new Configuration();
        } else {
            this.flussConf = builder.flussConfig;
        }

        tablePath = new TablePath(builder.database, builder.tableName);
        flussConf.setString("bootstrap.servers", bootstrapServers);

        Connection connection = ConnectionFactory.createConnection(flussConf);
        Admin admin = connection.getAdmin();
        TableInfo tableInfo;
        try {
            tableInfo = admin.getTableInfo(tablePath).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        flussConf.addAll(tableInfo.getCustomProperties());
        flussConf.addAll(tableInfo.getProperties());

        isPartitioned = !tableInfo.getPartitionKeys().isEmpty();
        hasPrimaryKey = !tableInfo.getPrimaryKeys().isEmpty();

        sourceOutputType =
                projectedFields != null
                        ? tableInfo.getRowType().project(projectedFields)
                        : tableInfo.getRowType();

        LOG.info("Creating Fluss Source with Configuration: {}", flussConf);
    }

    @Internal
    public FlussSource(
            Configuration flussConf,
            TablePath tablePath,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            RowType sourceOutputType,
            @Nullable int[] projectedFields,
            OffsetsInitializer offsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            FlussDeserializationSchema<OUT> deserializationSchema,
            boolean streaming) {
        this.flussConf = flussConf;
        this.tablePath = tablePath;
        this.hasPrimaryKey = hasPrimaryKey;
        this.isPartitioned = isPartitioned;
        this.sourceOutputType = sourceOutputType;
        this.projectedFields = projectedFields;
        this.offsetsInitializer = offsetsInitializer;
        this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
        this.deserializationSchema = deserializationSchema;
        this.streaming = streaming;
    }

    @Override
    public Boundedness getBoundedness() {
        return streaming ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<SourceSplitBase, SourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<SourceSplitBase> splitEnumeratorContext) {
        return new FlinkSourceEnumerator(
                tablePath,
                flussConf,
                hasPrimaryKey,
                isPartitioned,
                splitEnumeratorContext,
                offsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                streaming);
    }

    @Override
    public SplitEnumerator<SourceSplitBase, SourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<SourceSplitBase> splitEnumeratorContext,
            SourceEnumeratorState sourceEnumeratorState) {
        return new FlinkSourceEnumerator(
                tablePath,
                flussConf,
                hasPrimaryKey,
                isPartitioned,
                splitEnumeratorContext,
                sourceEnumeratorState.getAssignedBuckets(),
                sourceEnumeratorState.getAssignedPartitions(),
                offsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                streaming);
    }

    @Override
    public SimpleVersionedSerializer<SourceSplitBase> getSplitSerializer() {
        return SourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<SourceEnumeratorState> getEnumeratorCheckpointSerializer() {
        return FlussSourceEnumeratorStateSerializer.INSTANCE;
    }

    @Override
    public SourceReader<OUT, SourceSplitBase> createReader(SourceReaderContext context) {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<RecordAndPos>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        FlinkSourceReaderMetrics flinkSourceReaderMetrics =
                new FlinkSourceReaderMetrics(context.metricGroup());
        return new FlinkSourceReader(
                elementsQueue,
                flussConf,
                tablePath,
                sourceOutputType,
                context,
                projectedFields,
                flinkSourceReaderMetrics,
                deserializationSchema);
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    // https://alibaba.github.io/fluss-docs/docs/engine-flink/options/#read-options
    public static class Builder<T> {
        private String bootstrapServers;
        private String database;
        private String tableName;
        private OffsetsInitializer offsetsInitializer;
        private Long scanPartitionDiscoveryIntervalMs;
        private FlussDeserializationSchema<T> deserializationSchema;

        private Configuration flussConfig;
        private int[] projectedFields;

        private boolean isStreaming = true;

        public Builder<T> setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder<T> setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder<T> setTable(String table) {
            this.tableName = table;
            return this;
        }

        public Builder<T> setScanPartitionDiscoveryIntervalMs(
                long scanPartitionDiscoveryIntervalMs) {
            this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
            return this;
        }

        public Builder<T> setStartingOffsets(OffsetsInitializer offsetsInitializer) {
            this.offsetsInitializer = offsetsInitializer;
            return this;
        }

        public Builder<T> setDeserializationSchema(
                FlussDeserializationSchema<T> deserializationSchema) {
            this.deserializationSchema = deserializationSchema;
            return this;
        }

        public Builder<T> setIsBatch(boolean isBatch) {
            if (isBatch) {
                this.isStreaming = false;
            }
            return this;
        }

        public Builder<T> setProjectedFields(int[] projectedFields) {
            this.projectedFields = projectedFields;
            return this;
        }

        public Builder<T> setFlussConfig(Configuration flinkConfig) {
            this.flussConfig = flinkConfig;
            return this;
        }

        public FlussSource<T> build() {
            if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                throw new IllegalArgumentException("bootstrapServers must not be empty");
            }

            if (database == null || database.isEmpty()) {
                throw new IllegalArgumentException("database must not be empty");
            }

            if (tableName == null || tableName.isEmpty()) {
                throw new IllegalArgumentException("tableName must not be empty");
            }

            if (scanPartitionDiscoveryIntervalMs == null) {
                throw new IllegalArgumentException(
                        "scanPartitionDiscoveryIntervalMs must not be null");
            }

            if (offsetsInitializer == null) {
                throw new IllegalArgumentException("offsetsInitializer must not be null");
            }

            if (deserializationSchema == null) {
                throw new IllegalArgumentException("deserializationSchema must not be null");
            }

            return new FlussSource<>(this);
        }
    }
}
