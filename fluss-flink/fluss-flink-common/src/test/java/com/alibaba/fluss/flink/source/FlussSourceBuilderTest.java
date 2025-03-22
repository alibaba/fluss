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

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.serdes.FlussDeserializationSchema;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.row.InternalRow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class FlussSourceBuilderTest extends FlinkTestBase {

    private static String bootstrapServers;

    @BeforeEach
    public void setup() throws Exception {
        bootstrapServers = conn.getConfiguration().get(ConfigOptions.BOOTSTRAP_SERVERS).get(0);

        createTable(DEFAULT_TABLE_PATH, DEFAULT_PK_TABLE_DESCRIPTOR);
    }

    @Test
    public void testBuildWithValidConfiguration() {
        // Given
        FlussSource<TestRecord> source =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .build();

        // Then
        Assertions.assertNotNull(source, "FlussSource should be created successfully");
    }

    @Test
    public void testMissingBootstrapServers() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setDatabase(DEFAULT_DB)
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        executable,
                        "Should throw exception when bootstrapServers is missing");

        Assertions.assertTrue(
                exception.getMessage().contains("bootstrapServers must not be empty"));
    }

    @Test
    public void testEmptyBootstrapServers() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers("")
                                .setDatabase(DEFAULT_DB)
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        executable,
                        "Should throw exception when bootstrapServers is empty");

        Assertions.assertTrue(
                exception.getMessage().contains("bootstrapServers must not be empty"));
    }

    @Test
    public void testMissingDatabase() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        executable,
                        "Should throw exception when database is missing");

        Assertions.assertTrue(exception.getMessage().contains("database must not be empty"));
    }

    @Test
    public void testEmptyDatabase() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setDatabase("")
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        executable,
                        "Should throw exception when database is empty");

        Assertions.assertTrue(exception.getMessage().contains("database must not be empty"));
    }

    @Test
    public void testMissingTable() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setDatabase(DEFAULT_DB)
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        executable,
                        "Should throw exception when table is missing");

        Assertions.assertTrue(exception.getMessage().contains("tableName must not be empty"));
    }

    @Test
    public void testEmptyTable() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setDatabase(DEFAULT_DB)
                                .setTable("")
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        executable,
                        "Should throw exception when table is empty");

        Assertions.assertTrue(exception.getMessage().contains("tableName must not be empty"));
    }

    @Test
    public void testMissingScanPartitionDiscoveryInterval() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setDatabase(DEFAULT_DB)
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        executable,
                        "Should throw exception when scanPartitionDiscoveryIntervalMs is missing");

        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains("scanPartitionDiscoveryIntervalMs must not be null"));
    }

    @Test
    public void testMissingOffsetsInitializer() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setDatabase(DEFAULT_DB)
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        executable,
                        "Should throw exception when offsetsInitializer is missing");

        Assertions.assertTrue(
                exception.getMessage().contains("offsetsInitializer must not be null"));
    }

    @Test
    public void testMissingDeserializationSchema() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setDatabase(DEFAULT_DB)
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .build();

        // Then
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        executable,
                        "Should throw exception when deserializationSchema is missing");

        Assertions.assertTrue(
                exception.getMessage().contains("deserializationSchema must not be null"));
    }

    @Test
    public void testSetProjectedFields() {
        // Given
        int[] projectedFields = new int[] {0, 1};
        FlussSource<TestRecord> source =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .setProjectedFields(projectedFields)
                        .build();

        // Then
        Assertions.assertNotNull(
                source, "FlussSource should be created successfully with projected fields");
    }

    @Test
    public void testSetBatchMode() {
        // Given
        FlussSource<TestRecord> source =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .setIsBatch(true)
                        .build();

        // Then
        Assertions.assertNotNull(
                source, "FlussSource should be created successfully in batch mode");
    }

    @Test
    public void testSetFlussConfig() {
        // Given
        Configuration customConfig = new Configuration();
        customConfig.setString(ConfigOptions.CLIENT_SCANNER_LOG_CHECK_CRC.key(), "false");
        customConfig.setLong(ConfigOptions.CLIENT_SCANNER_LOG_FETCH_MAX_BYTES.key(), 1048576L);
        customConfig.setLong(ConfigOptions.CLIENT_SCANNER_LOG_MAX_POLL_RECORDS.key(), 100);
        customConfig.setString(
                ConfigOptions.CLIENT_SCANNER_LOG_FETCH_MAX_BYTES_FOR_BUCKET.key(), "2mb");

        FlussSource<TestRecord> source =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .setFlussConfig(customConfig)
                        .build();

        // Then
        Assertions.assertNotNull(
                source, "FlussSource should be created successfully with custom configuration");
    }

    @Test
    public void testProjectedFields() {
        // Given
        int[] projectedFields = new int[] {0, 1}; // Only include orderId and amount fields

        // When
        FlussSource<TestRecord> source =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .setProjectedFields(projectedFields)
                        .build();

        // Then
        Assertions.assertNotNull(
                source, "FlussSource should be created successfully with projected fields");
    }

    // Test record class for tests
    private static class TestRecord {
        private int id;
        private String name;

        public TestRecord(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }

    // Test deserialization schema for tests
    private static class TestDeserializationSchema
            implements FlussDeserializationSchema<TestRecord> {
        @Override
        public TestRecord deserialize(ScanRecord flussRecord) {
            InternalRow row = flussRecord.getRow();
            return new TestRecord(row.getInt(0), row.getString(1).toString());
        }

        @Override
        public TypeInformation<TestRecord> getProducedType() {
            return TypeInformation.of(TestRecord.class);
        }
    }
}
