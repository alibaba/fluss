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

package com.alibaba.fluss.flink.sink.converter;

import com.alibaba.fluss.flink.source.deserializer.Order;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ConvertingSinkWriter}. */
class ConvertingSinkWriterTest {

    private ConvertingSinkWriter<Order> convertingSinkWriter;
    private TestSinkWriter testSinkWriter;
    private RowDataConverter<Order> converter;
    private SinkWriter.Context testContext;

    @BeforeEach
    void setUp() {
        testSinkWriter = new TestSinkWriter();
        converter = new OrderRecordConverter();
        convertingSinkWriter = new ConvertingSinkWriter<>(testSinkWriter, converter);

        // Simple implementation of Context
        testContext =
                new SinkWriter.Context() {
                    @Override
                    public long currentWatermark() {
                        return 0;
                    }

                    @Override
                    public Long timestamp() {
                        return 0L;
                    }
                };
    }

    @Test
    void testWriteWithSuccessfulConversion() throws IOException {
        // Given
        Order record = new Order(1000L, 101L, 20, "Test 123 Main St");

        // When
        convertingSinkWriter.write(record, testContext);

        // Then
        List<RowData> writtenRows = testSinkWriter.getWrittenRows();
        assertThat(writtenRows).hasSize(1);

        RowData writtenRow = writtenRows.get(0);
        assertThat(writtenRow.getLong(0)).isEqualTo(1000L);
        assertThat(writtenRow.getLong(1)).isEqualTo(101L);
        assertThat(writtenRow.getInt(2)).isEqualTo(20);
        assertThat(writtenRow.getString(3).toString()).isEqualTo("Test 123 Main St");
    }

    @Test
    void testWriteWithRowDataInput() throws IOException {
        // Given
        GenericRowData rowData = new GenericRowData(4);
        rowData.setField(0, 1000L);
        rowData.setField(1, 101L);
        rowData.setField(2, 20);
        rowData.setField(3, StringData.fromString("Test 123 Main St"));

        ConvertingSinkWriter<RowData> rowDataWriter =
                new ConvertingSinkWriter<>(
                        testSinkWriter,
                        // Identity converter that just returns the row data
                        (RowDataConverter<RowData>) input -> input);

        // When
        rowDataWriter.write(rowData, testContext);

        // Then
        List<RowData> writtenRows = testSinkWriter.getWrittenRows();
        assertThat(writtenRows).hasSize(1);
        assertThat(writtenRows.get(0)).isSameAs(rowData);
    }

    @Test
    void testWriteMultipleRecords() throws IOException {
        // Given
        Order record1 = new Order(1000L, 101L, 20, "Test 123 Main St");
        Order record2 = new Order(1001L, 102L, 45, "Test 123 Main St");
        Order record3 = new Order(1002L, 103L, 38, "Test 123 Main St");

        // When
        convertingSinkWriter.write(record1, testContext);
        convertingSinkWriter.write(record2, testContext);
        convertingSinkWriter.write(record3, testContext);

        // Then
        List<RowData> writtenRows = testSinkWriter.getWrittenRows();
        assertThat(writtenRows).hasSize(3);

        assertThat(writtenRows.get(0).getLong(0)).isEqualTo(1000L);
        assertThat(writtenRows.get(0).getLong(1)).isEqualTo(101L);
        assertThat(writtenRows.get(0).getInt(2)).isEqualTo(20);
        assertThat(writtenRows.get(0).getString(3).toString()).isEqualTo("Test 123 Main St");

        assertThat(writtenRows.get(1).getLong(0)).isEqualTo(1001L);
        assertThat(writtenRows.get(1).getLong(1)).isEqualTo(102L);
        assertThat(writtenRows.get(1).getInt(2)).isEqualTo(45);
        assertThat(writtenRows.get(1).getString(3).toString()).isEqualTo("Test 123 Main St");

        assertThat(writtenRows.get(2).getLong(0)).isEqualTo(1002L);
        assertThat(writtenRows.get(2).getLong(1)).isEqualTo(103L);
        assertThat(writtenRows.get(2).getInt(2)).isEqualTo(38);
        assertThat(writtenRows.get(2).getString(3).toString()).isEqualTo("Test 123 Main St");
    }

    @Test
    void testWriteWithNullElement() throws IOException {
        // When
        convertingSinkWriter.write(null, testContext);

        // Then
        List<RowData> writtenRows = testSinkWriter.getWrittenRows();
        assertThat(writtenRows).hasSize(1);
        assertThat(writtenRows.get(0)).isNull();
    }

    @Test
    void testFlush() throws IOException, InterruptedException {
        // When
        convertingSinkWriter.flush(true);

        // Then
        assertThat(testSinkWriter.isFlushed()).isTrue();
    }

    @Test
    void testClose() throws Exception {
        // When
        convertingSinkWriter.close();

        // Then
        assertThat(testSinkWriter.isClosed()).isTrue();
    }

    // Custom RowDataConverter implementation
    static class OrderRecordConverter implements RowDataConverter<Order> {
        @Override
        public RowData convert(Order order) throws Exception {
            if (order == null) {
                return null;
            }

            //            if (order.getName().equals("error")) {
            //                throw new RuntimeException("Simulated conversion error");
            //            }

            GenericRowData rowData = new GenericRowData(4);
            rowData.setField(0, order.getOrderId());
            rowData.setField(1, order.getItemId());
            rowData.setField(2, order.getAmount());
            rowData.setField(3, StringData.fromString(order.getAddress()));
            return rowData;
        }
    }

    // Test implementation of SinkWriter that stores written records
    static class TestSinkWriter implements SinkWriter<RowData> {
        private final List<RowData> writtenRows = new ArrayList<>();
        private boolean flushed = false;
        private boolean closed = false;
        private boolean failOnWrite = false;

        @Override
        public void write(RowData element, Context context) throws IOException {
            if (failOnWrite) {
                throw new IOException("Simulated write error");
            }
            writtenRows.add(element);
        }

        @Override
        public void flush(boolean endOfInput) {
            flushed = true;
        }

        @Override
        public void close() {
            closed = true;
        }

        public List<RowData> getWrittenRows() {
            return writtenRows;
        }

        public boolean isFlushed() {
            return flushed;
        }

        public boolean isClosed() {
            return closed;
        }

        public void setFailOnWrite(boolean failOnWrite) {
            this.failOnWrite = failOnWrite;
        }
    }
}
