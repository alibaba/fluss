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

package com.alibaba.fluss.flink.source.deserializer;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.record.LogRecord;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.util.HashMap;
import java.util.Map;

/**
 * A deserialization schema that converts {@link LogRecord} objects to JSON strings.
 *
 * <p>This implementation serializes Fluss records into JSON strings, making it useful for
 * debugging, logging, or when the downstream processing requires string-based JSON data. The schema
 * preserves important metadata such as offset, timestamp, and change type along with the actual row
 * data.
 *
 * <p>The resulting JSON has the following structure:
 *
 * <pre>{@code
 * {
 *   "offset": <record_offset>,
 *   "timestamp": <record_timestamp>,
 *   "changeType": <APPEND_ONLY|INSERT|UPDATE_BEFORE|UPDATE_AFTER|DELETE>,
 *   "row": <string_representation_of_row>
 * }
 * }</pre>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * JsonStringDeserializationSchema schema = new JsonStringDeserializationSchema();
 * FlussSource<String> source = FlussSource.builder()
 *     .setDeserializationSchema(schema)
 *     .build();
 * }</pre>
 *
 * @since 0.7
 */
@PublicEvolving
public class JsonStringDeserializationSchema implements FlussDeserializationSchema<String> {
    private static final long serialVersionUID = 1L;

    /**
     * Jackson ObjectMapper used for JSON serialization. Marked as transient because ObjectMapper is
     * not serializable and needs to be recreated in the open method.
     */
    private transient ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Reusable map for building the record representation before serializing to JSON. This avoids
     * creating a new Map for each record.
     */
    private final Map<String, Object> recordMap = new HashMap<>(4);

    /**
     * Initializes the JSON serialization mechanism.
     *
     * <p>This method creates a new ObjectMapper instance and configures it with:
     *
     * <ul>
     *   <li>JavaTimeModule for proper serialization of date/time objects
     *   <li>Configuration to render dates in ISO-8601 format rather than timestamps
     * </ul>
     *
     * @param context Contextual information for initialization (not used in this implementation)
     * @throws Exception if initialization fails
     */
    @Override
    public void open(InitializationContext context) throws Exception {
        objectMapper = new ObjectMapper();

        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    /**
     * Deserializes a {@link LogRecord} into a JSON {@link String}.
     *
     * <p>The method extracts key information from the record (offset, timestamp, change type, and
     * row data) and serializes it as a JSON string.
     *
     * @param record The Fluss LogRecord to deserialize
     * @return JSON string representation of the record
     * @throws Exception If JSON serialization fails
     */
    @Override
    public String deserialize(LogRecord record) throws Exception {
        recordMap.put("offset", record.logOffset());
        recordMap.put("timestamp", record.timestamp());
        recordMap.put("change_type", record.getChangeType().toString());
        recordMap.put("row", record.getRow().toString());

        return objectMapper.writeValueAsString(recordMap);
    }

    /**
     * Returns the TypeInformation for the produced {@link String} type.
     *
     * @return TypeInformation for String class
     */
    @Override
    public TypeInformation<String> getProducedType() {
        return Types.STRING;
    }
}
