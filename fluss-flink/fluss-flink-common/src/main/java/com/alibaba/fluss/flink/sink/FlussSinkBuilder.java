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

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.sink.serializer.FlussSerializationSchema;
import com.alibaba.fluss.flink.sink.writer.FlinkSinkWriter;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * Builder for creating and configuring Fluss sink connectors for Apache Flink.
 *
 * <p>The builder supports automatic schema inference from POJO classes using reflection and
 * provides options for customizing data conversion logic through custom converters.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * FlinkSink<Order> sink = new FlussSinkBuilder<Order>()
 *          .setBootstrapServers(bootstrapServers)
 *          .setTable(tableName)
 *          .setRowType(orderRowType)
 *          .setSerializationSchema(new OrderSerializationSchema())
 *          .build())
 * }</pre>
 *
 * @param <InputT>> The input type of records to be written to Fluss
 */
public class FlussSinkBuilder<InputT> {
    private static final Logger LOG = LoggerFactory.getLogger(FlussSinkBuilder.class);

    private String bootstrapServers;
    private String database;
    private String tableName;
    private RowType tableRowType;
    private boolean ignoreDelete;
    private int[] partialUpdateColumns;
    private boolean isUpsert;
    private final Map<String, String> configOptions = new HashMap<>();
    private FlussSerializationSchema<InputT> serializationSchema;
    private boolean shuffleByBucketId = true;
    private @Nullable DataLakeFormat lakeFormat;

    /** Set the bootstrap server for the sink. */
    public FlussSinkBuilder<InputT> setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    /** Set the database for the sink. */
    public FlussSinkBuilder<InputT> setDatabase(String database) {
        this.database = database;
        return this;
    }

    /** Set the table name for the sink. */
    public FlussSinkBuilder<InputT> setTable(String table) {
        this.tableName = table;
        return this;
    }

    /** Set the row type for the sink. */
    public FlussSinkBuilder<InputT> setRowType(RowType rowType) {
        this.tableRowType = rowType;
        return this;
    }

    /** Set whether to ignore delete operations. */
    public FlussSinkBuilder<InputT> setIgnoreDelete(boolean ignoreDelete) {
        this.ignoreDelete = ignoreDelete;
        return this;
    }

    /** Set target column indexes. */
    public FlussSinkBuilder<InputT> setPartialUpdateColumns(int[] partialUpdateColumns) {
        this.partialUpdateColumns = partialUpdateColumns;
        return this;
    }

    /** Set target column indexes. */
    public FlussSinkBuilder<InputT> setDataLakeFormat(DataLakeFormat lakeFormat) {
        this.lakeFormat = lakeFormat;
        return this;
    }

    /** Set shuffle by bucket id. */
    public FlussSinkBuilder<InputT> setShuffleByBucketId(boolean shuffleByBucketId) {
        if (!shuffleByBucketId) {
            this.shuffleByBucketId = false;
        }
        return this;
    }

    /** Configure this sink to use upsert semantics. */
    public FlussSinkBuilder<InputT> useUpsert() {
        this.isUpsert = true;
        return this;
    }

    /** Configure this sink to use append-only semantics. */
    public FlussSinkBuilder<InputT> useAppend() {
        this.isUpsert = false;
        return this;
    }

    /** Set a configuration option. */
    public FlussSinkBuilder<InputT> setOption(String key, String value) {
        configOptions.put(key, value);
        return this;
    }

    /** Set multiple configuration options. */
    public FlussSinkBuilder<InputT> setOptions(Map<String, String> options) {
        configOptions.putAll(options);
        return this;
    }

    /** Set a FlussSerializationSchema. */
    public FlussSinkBuilder<InputT> setSerializationSchema(
            FlussSerializationSchema<InputT> serializationSchema) {
        this.serializationSchema = serializationSchema;
        return this;
    }

    /** Build the FlussSink. */
    public FlinkSink<InputT> build() {
        validateConfiguration();

        Configuration flussConfig = Configuration.fromMap(configOptions);

        FlinkSink.SinkWriterBuilder<? extends FlinkSinkWriter<InputT>, InputT> writerBuilder;

        TablePath tablePath = new TablePath(database, tableName);
        flussConfig.setString("bootstrap.servers", bootstrapServers);

        TableInfo tableInfo;
        try (Connection connection = ConnectionFactory.createConnection(flussConfig);
                Admin admin = connection.getAdmin()) {
            try {
                tableInfo = admin.getTableInfo(tablePath).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while getting table info", e);
            } catch (ExecutionException e) {
                throw new RuntimeException("Failed to get table info", e);
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to initialize FlussSource admin connection: " + e.getMessage(), e);
        }
        int numBucket = tableInfo.getNumBuckets();
        List<String> bucketKeys = tableInfo.getBucketKeys();
        List<String> partitionKeys = tableInfo.getPartitionKeys();

        if (isUpsert) {
            LOG.info("Initializing Fluss upsert sink writer ...");
            writerBuilder =
                    new FlinkSink.UpsertSinkWriterBuilder<>(
                            tablePath,
                            flussConfig,
                            tableRowType,
                            partialUpdateColumns,
                            numBucket,
                            bucketKeys,
                            partitionKeys,
                            lakeFormat,
                            shuffleByBucketId,
                            serializationSchema);
        } else {
            LOG.info("Initializing Fluss append sink writer ...");
            writerBuilder =
                    new FlinkSink.AppendSinkWriterBuilder<>(
                            tablePath,
                            flussConfig,
                            tableRowType,
                            numBucket,
                            bucketKeys,
                            partitionKeys,
                            lakeFormat,
                            shuffleByBucketId,
                            serializationSchema);
        }

        return new FlussSink<>(writerBuilder);
    }

    private void validateConfiguration() {
        checkNotNull(bootstrapServers, "BootstrapServers is required but not provided.");
        checkNotNull(serializationSchema, "SerializationSchema is required but not provided.");
        checkNotNull(tableRowType, "Table row type is required but not provided.");

        checkNotNull(database, "Database is required but not provided.");
        checkArgument(!database.isEmpty(), "Database cannot be empty.");

        checkNotNull(tableName, "Table name is required but not provided.");
        checkArgument(!tableName.isEmpty(), "Table name cannot be empty.");

        checkArgument(
                isUpsert && partialUpdateColumns == null,
                "Partial updates are not supported in append mode.");
    }
}
