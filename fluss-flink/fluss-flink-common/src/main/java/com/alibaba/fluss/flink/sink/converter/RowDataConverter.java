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

import org.apache.flink.table.data.RowData;

import java.io.Serializable;

/**
 * Converter that transforms objects of type T into Flink's internal {@link RowData} representation.
 *
 * <p>This interface serves as a key component in Fluss data sink operations, enabling custom
 * transformation logic when writing application objects to Fluss tables.
 *
 * <p>Implementations of this interface must be serializable as they may be distributed across a
 * Flink cluster during job execution.
 *
 * <p>Usage examples:
 *
 * <ul>
 *   <li>Converting POJO objects to {@link RowData} for sink operations
 *   <li>Transforming data during the write process (e.g., field mapping, data filtering)
 *   <li>Implementing custom serialization strategies for complex data types
 * </ul>
 *
 * @param <T> The input type to be converted to {@link RowData}
 * @see org.apache.flink.table.data.RowData
 * @see com.alibaba.fluss.flink.sink.FlinkSink
 */
public interface RowDataConverter<T> extends Serializable {
    /**
     * Converts an element of type T to Flink's internal {@link RowData} representation.
     *
     * @param element The input element to convert
     * @return The converted {@link RowData} representation of the input element
     * @throws Exception If any error occurs during conversion
     */
    RowData convert(T element) throws Exception;
}
