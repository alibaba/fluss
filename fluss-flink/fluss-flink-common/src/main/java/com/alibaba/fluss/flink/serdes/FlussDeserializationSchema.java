/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.serdes;

import com.alibaba.fluss.client.table.scanner.ScanRecord;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;

/**
 * Interface for deserialization schema used to deserialize {@link
 * com.alibaba.fluss.client.table.scanner.ScanRecord} objects into specific data types.
 *
 * @param <T> The type created by the deserialization schema.
 */
public interface FlussDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Deserializes a {@link ScanRecord} into an object of type T.
     *
     * @param scanRecord The record to deserialize.
     * @return The deserialized object.
     * @throws Exception If the deserialization fails.
     */
    T deserialize(ScanRecord scanRecord) throws Exception;
}
