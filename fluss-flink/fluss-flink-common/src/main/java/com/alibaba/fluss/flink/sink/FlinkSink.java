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

package com.alibaba.fluss.flink.sink;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.sink.converter.ConvertingSinkWriter;
import com.alibaba.fluss.flink.sink.converter.RowDataConverter;
import com.alibaba.fluss.flink.sink.writer.AppendSinkWriter;
import com.alibaba.fluss.flink.sink.writer.FlinkSinkWriter;
import com.alibaba.fluss.flink.sink.writer.UpsertSinkWriter;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import static com.alibaba.fluss.flink.sink.FlinkStreamPartitioner.partition;
import static com.alibaba.fluss.flink.utils.FlinkConversions.toFlussRowType;

/** Flink sink for Fluss. */
class FlinkSink<InputT> implements Sink<InputT>, SupportsPreWriteTopology<InputT> {

    private static final long serialVersionUID = 1L;

    private final SinkWriterBuilder<? extends FlinkSinkWriter> builder;

    private final RowDataConverter<InputT> converter;

    public FlinkSink(SinkWriterBuilder<? extends FlinkSinkWriter> builder) {
        this.builder = builder;
        this.converter = element -> (RowData) element;
    }

    private FlinkSink(
            SinkWriterBuilder<? extends FlinkSinkWriter> builder,
            RowDataConverter<InputT> converter) {
        this.builder = builder;
        this.converter = converter;
    }

    @Deprecated
    @Override
    public SinkWriter<InputT> createWriter(InitContext context) throws IOException {
        FlinkSinkWriter flinkSinkWriter = builder.createWriter();
        flinkSinkWriter.initialize(InternalSinkWriterMetricGroup.wrap(context.metricGroup()));
        return new ConvertingSinkWriter<>(flinkSinkWriter, converter);
    }

    @Override
    public SinkWriter<InputT> createWriter(WriterInitContext context) throws IOException {
        FlinkSinkWriter flinkSinkWriter = builder.createWriter();
        flinkSinkWriter.initialize(InternalSinkWriterMetricGroup.wrap(context.metricGroup()));
        return new ConvertingSinkWriter<>(flinkSinkWriter, converter);
    }

    @Override
    public DataStream<InputT> addPreWriteTopology(DataStream<InputT> input) {
        // Check if input is already RowData (optimization for common case)
        if (input.getType().getTypeClass() == RowData.class) {
            @SuppressWarnings("unchecked")
            DataStream<RowData> rowDataInput = (DataStream<RowData>) input;
            DataStream<RowData> processed = builder.addPreWriteTopology(rowDataInput);

            // If the builder didn't change the stream, return the original input
            if (processed == rowDataInput) {
                return input;
            }

            // Otherwise cast the processed stream to InputT
            @SuppressWarnings("unchecked")
            DataStream<InputT> result = (DataStream<InputT>) processed;
            return result;
        }

        // For non-RowData input, convert to RowData first
        DataStream<RowData> rowDataInput =
                input.map(
                        (MapFunction<InputT, RowData>) converter::convert,
                        org.apache.flink.api.common.typeinfo.TypeInformation.of(RowData.class));

        DataStream<RowData> processed = builder.addPreWriteTopology(rowDataInput);

        // Convert back to original type.
        @SuppressWarnings("unchecked")
        DataStream<InputT> result = (DataStream<InputT>) processed;
        return result;
    }

    @Internal
    interface SinkWriterBuilder<W extends FlinkSinkWriter> extends Serializable {
        W createWriter();

        DataStream<RowData> addPreWriteTopology(DataStream<RowData> input);
    }

    /** A SinkWriter that converts the input type to RowData before writing. */
    @Internal
    static class AppendSinkWriterBuilder implements SinkWriterBuilder<AppendSinkWriter> {

        private static final long serialVersionUID = 1L;

        private final TablePath tablePath;
        private final Configuration flussConfig;
        private final RowType tableRowType;
        private final boolean ignoreDelete;
        private final int numBucket;
        private final List<String> bucketKeys;
        private final List<String> partitionKeys;
        private final @Nullable DataLakeFormat lakeFormat;
        private final boolean shuffleByBucketId;

        public AppendSinkWriterBuilder(
                TablePath tablePath,
                Configuration flussConfig,
                RowType tableRowType,
                boolean ignoreDelete,
                int numBucket,
                List<String> bucketKeys,
                List<String> partitionKeys,
                @Nullable DataLakeFormat lakeFormat,
                boolean shuffleByBucketId) {
            this.tablePath = tablePath;
            this.flussConfig = flussConfig;
            this.tableRowType = tableRowType;
            this.ignoreDelete = ignoreDelete;
            this.numBucket = numBucket;
            this.bucketKeys = bucketKeys;
            this.partitionKeys = partitionKeys;
            this.lakeFormat = lakeFormat;
            this.shuffleByBucketId = shuffleByBucketId;
        }

        @Override
        public AppendSinkWriter createWriter() {
            return new AppendSinkWriter(tablePath, flussConfig, tableRowType, ignoreDelete);
        }

        @Override
        public DataStream<RowData> addPreWriteTopology(DataStream<RowData> input) {
            // For append only sink, we will do bucket shuffle only if bucket keys are not empty.
            if (!bucketKeys.isEmpty() && shuffleByBucketId) {
                return partition(
                        input,
                        new FlinkRowDataChannelComputer(
                                toFlussRowType(tableRowType),
                                bucketKeys,
                                partitionKeys,
                                lakeFormat,
                                numBucket),
                        input.getParallelism());
            } else {
                return input;
            }
        }
    }

    @Internal
    static class UpsertSinkWriterBuilder implements SinkWriterBuilder<UpsertSinkWriter> {

        private static final long serialVersionUID = 1L;

        private final TablePath tablePath;
        private final Configuration flussConfig;
        private final RowType tableRowType;
        private final @Nullable int[] targetColumnIndexes;
        private final boolean ignoreDelete;
        private final int numBucket;
        private final List<String> bucketKeys;
        private final List<String> partitionKeys;
        private final @Nullable DataLakeFormat lakeFormat;
        private final boolean shuffleByBucketId;

        UpsertSinkWriterBuilder(
                TablePath tablePath,
                Configuration flussConfig,
                RowType tableRowType,
                @Nullable int[] targetColumnIndexes,
                boolean ignoreDelete,
                int numBucket,
                List<String> bucketKeys,
                List<String> partitionKeys,
                @Nullable DataLakeFormat lakeFormat,
                boolean shuffleByBucketId) {
            this.tablePath = tablePath;
            this.flussConfig = flussConfig;
            this.tableRowType = tableRowType;
            this.targetColumnIndexes = targetColumnIndexes;
            this.ignoreDelete = ignoreDelete;
            this.numBucket = numBucket;
            this.bucketKeys = bucketKeys;
            this.partitionKeys = partitionKeys;
            this.lakeFormat = lakeFormat;
            this.shuffleByBucketId = shuffleByBucketId;
        }

        @Override
        public UpsertSinkWriter createWriter() {
            return new UpsertSinkWriter(
                    tablePath, flussConfig, tableRowType, targetColumnIndexes, ignoreDelete);
        }

        @Override
        public DataStream<RowData> addPreWriteTopology(DataStream<RowData> input) {
            return shuffleByBucketId
                    ? partition(
                            input,
                            new FlinkRowDataChannelComputer(
                                    toFlussRowType(tableRowType),
                                    bucketKeys,
                                    partitionKeys,
                                    lakeFormat,
                                    numBucket),
                            input.getParallelism())
                    : input;
        }
    }
}
