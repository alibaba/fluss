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

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

/**
 * A {@link SinkWriter} implementation that converts input elements to {@link RowData} before
 * writing.
 *
 * <p>This class acts as an adapter between a source-provided data type {@code InputT} and the
 * internal {@link RowData} format required by Fluss sink writers. It delegates actual writing
 * operations to an underlying {@link RowData}-based sink writer after performing the conversion.
 *
 * <p>The conversion is handled by a {@link RowDataConverter} which is responsible for transforming
 * objects of type {@code InputT} into Flink's internal {@link RowData} representation. If the input
 * element is already a {@link RowData} instance, it's used directly without conversion.
 *
 * <p>This allows Fluss sinks to work with various input types while maintaining a consistent
 * internal representation for data processing and storage operations.
 *
 * @param <InputT> The type of elements accepted by this sink writer
 * @see RowDataConverter
 * @see RowData
 * @see SinkWriter
 */
public class ConvertingSinkWriter<InputT> implements SinkWriter<InputT> {
    private final SinkWriter<RowData> rowDataWriter;
    private final RowDataConverter<InputT> converter;

    public ConvertingSinkWriter(
            SinkWriter<RowData> rowDataWriter, RowDataConverter<InputT> converter) {
        this.rowDataWriter = rowDataWriter;
        this.converter = converter;
    }

    @Override
    public void write(InputT element, Context context) throws IOException {
        RowData rowData;
        try {
            if (!(element instanceof RowData)) {
                rowData = converter.convert(element);

            } else {
                rowData = (RowData) element;
            }
            rowDataWriter.write(rowData, context);
        } catch (Exception e) {
            throw new IOException(
                    String.format(
                            "Failed to convert element of type '%s' to RowData: %s",
                            element != null ? element.getClass().getName() : "null",
                            e.getMessage()),
                    e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        rowDataWriter.flush(endOfInput);
    }

    @Override
    public void close() throws Exception {
        rowDataWriter.close();
    }
}
