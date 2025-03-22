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

package com.alibaba.fluss.flink.serdes;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.flink.utils.FlussRowToFlinkRowConverter;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;

/**
 * A deserialization schema that converts Fluss {@link ScanRecord} objects to Flink's {@link
 * RowData}.
 *
 * <p>This implementation uses a {@link FlussRowToFlinkRowConverter} to efficiently transform Fluss
 * row representations into Flink's row format without unnecessary intermediate conversions. It's
 * optimized for direct integration between Fluss data sources and Flink's processing pipeline.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * RowType rowType = ...; // Define your row type
 * FlussRowDeserializationSchema schema = new FlussRowDeserializationSchema(rowType);
 * RowData flinkRow = schema.deserialize(scanRecord);
 * }</pre>
 *
 * @see FlussDeserializationSchema
 * @see FlussRowToFlinkRowConverter
 * @see ScanRecord
 */
public class FlussRowDeserializationSchema implements FlussDeserializationSchema<RowData> {
    private final FlussRowToFlinkRowConverter converter;

    public FlussRowDeserializationSchema(RowType rowType) {
        this.converter = new FlussRowToFlinkRowConverter(rowType);
    }

    @Override
    public RowData deserialize(ScanRecord scanRecord) throws Exception {
        return converter.toFlinkRowData(scanRecord);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return TypeInformation.of(RowData.class);
    }
}
