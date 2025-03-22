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

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.flink.helper.Order;
import com.alibaba.fluss.flink.row.RowConverters;
import com.alibaba.fluss.flink.serdes.FlussRowDeserializationSchema;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.flink.source.testutils.MockDataUtils;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.alibaba.fluss.testutils.DataTestUtils.row;

public class FlussSourceLogITCase extends FlinkTestBase {
    private static final List<Order> orders = MockDataUtils.ORDERS;

    private static final Schema logSchema = MockDataUtils.getOrdersSchemaLog();

    private static TableDescriptor logTableDescriptor;

    private static String bootstrapServers;

    private static String logTableName = "orders_test_log";

    private static TablePath ordersLogTablePath;

    @BeforeEach
    public void setup() throws Exception {
        bootstrapServers = conn.getConfiguration().get(ConfigOptions.BOOTSTRAP_SERVERS).get(0);

        logTableDescriptor =
                TableDescriptor.builder().schema(logSchema).distributedBy(1, "orderId").build();

        TablePath logTablePath = TablePath.of(DEFAULT_DB, logTableName);

        createTable(logTablePath, logTableDescriptor);

        ordersLogTablePath = new TablePath(DEFAULT_DB, logTableName);

        initTables();
        bootstrapServers = conn.getConfiguration().get(ConfigOptions.BOOTSTRAP_SERVERS).get(0);
    }

    @AfterAll
    protected static void afterAll() throws Exception {
        conn.close();
    }

    @Test
    public void testRowDataLogTableSource() throws Exception {
        FlinkTestBase.beforeAll();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Table table = conn.getTable(ordersLogTablePath);
        RowType rowType = table.getTableInfo().getRowType();

        // Create a DataStream from the FlussSource
        FlussSource<RowData> flussSource =
                FlussSource.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(logTableName)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new FlussRowDeserializationSchema(rowType))
                        .build();

        DataStreamSource<RowData> stream =
                env.fromSource(flussSource, WatermarkStrategy.noWatermarks(), "Fluss Source");

        // these rows should be interpreted as Inserts
        List<InternalRow> updatedRows =
                Arrays.asList(row(600L, 20L, 600, "addr1"), row(700L, 22L, 601, "addr2"));

        // send some row updates
        writeRows(ordersLogTablePath, updatedRows, true);

        List<RowData> expectedResult =
                Arrays.asList(
                        createRowData(600L, 20L, 600, "addr1", RowKind.INSERT),
                        createRowData(700L, 22L, 601, "addr2", RowKind.INSERT),
                        createRowData(800L, 23L, 602, "addr3", RowKind.INSERT),
                        createRowData(900L, 24L, 603, "addr4", RowKind.INSERT),
                        createRowData(1000L, 25L, 604, "addr5", RowKind.INSERT),
                        createRowData(600L, 20L, 600, "addr1", RowKind.INSERT),
                        createRowData(700L, 22L, 601, "addr2", RowKind.INSERT));

        List<RowData> collectedRows = new ArrayList<>();
        try (CloseableIterator<RowData> iterator = stream.collectAsync()) {
            env.executeAsync("Test Fluss Log RowData Source");
            int count = 0;
            while (iterator.hasNext() && count < expectedResult.size() - 1) {
                collectedRows.add(iterator.next());
                count++;
            }
            // at this point the iterator should have one more element
            collectedRows.add(iterator.next());
        }

        // Assert result size and elements match
        Assertions.assertEquals(expectedResult.size(), collectedRows.size());
        Assertions.assertEquals(expectedResult, collectedRows);
    }

    //    @Test
    //    public void testTableLogSourceWithProjectionPushdown() throws Exception {
    //        FlinkTestBase.beforeAll();
    //        List<OrderPartial> expectedOutput =   Arrays.asList(
    //                new OrderPartial(600, 600),
    //                new OrderPartial(700, 601),
    //                new OrderPartial(800, 602),
    //                new OrderPartial(900, 603),
    //                new OrderPartial(1000, 604));
    //
    //        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //        env.setParallelism(1);
    //
    //        // Create a DataStream from the FlussSource
    //        FlussSource<OrderPartial> flussSource =
    //                FlussSource.<OrderPartial>builder()
    //                        .setBootstrapServers(bootstrapServers)
    //                        .setDatabase(DEFAULT_DB)
    //                        .setTable(logTableName)
    //                        .setStartingOffsets(OffsetsInitializer.earliest())
    //                        .setScanPartitionDiscoveryIntervalMs(1000L)
    //                        .setDeserializationSchema(new OrderPartialDeserializationSchema())
    //                        .setProjectedFields(new int[] {0, 2})
    //                        .build();
    //
    //        DataStreamSource<OrderPartial> stream =
    //                env.fromSource(flussSource, WatermarkStrategy.noWatermarks(), "Fluss Source");
    //
    //        List<OrderPartial> collectedElements = new ArrayList<>();
    //        try (CloseableIterator<OrderPartial> data = stream.collectAsync()) {
    //            env.executeAsync("Test Fluss Log Orders Source With Projection Pushdown");
    //            int count = 0;
    //            while (data.hasNext() && count < expectedOutput.size() - 1) {
    //                collectedElements.add(data.next());
    //                count++;
    //            }
    //            collectedElements.add(data.next());
    //        }
    //
    //        // Assert result size and elements match
    //        Assertions.assertEquals(expectedOutput.size(), collectedElements.size());
    //        Assertions.assertEquals(expectedOutput, collectedElements);
    //    }

    private static RowData createRowData(
            Long orderId, Long itemId, Integer amount, String address, RowKind rowKind) {
        GenericRowData row = new GenericRowData(4);
        row.setField(0, orderId);
        row.setField(1, itemId);
        row.setField(2, amount);
        row.setField(3, StringData.fromString(address));

        row.setRowKind(rowKind);
        return row;
    }

    private void initTables() {
        List<GenericRow> rows =
                orders.stream().map(RowConverters::pojoToGenericRow).collect(Collectors.toList());

        Table logTable = conn.getTable(ordersLogTablePath);

        for (GenericRow row : rows) {
            try {
                logTable.newAppend().createWriter().append(row).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Failed to insert test data", e);
            }
        }
    }
}
