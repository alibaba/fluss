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

package com.alibaba.fluss.flink.source.testutils;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.flink.helper.Order;
import com.alibaba.fluss.flink.serdes.FlussDeserializationSchema;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Arrays;
import java.util.List;

public class MockDataUtils {
    public static final List<Order> ORDERS =
            Arrays.asList(
                    new Order(600, 20, 600, "addr1"),
                    new Order(700, 22, 601, "addr2"),
                    new Order(800, 23, 602, "addr3"),
                    new Order(900, 24, 603, "addr4"),
                    new Order(1000, 25, 604, "addr5"));

    public static Schema getOrdersSchemaPK() {
        return Schema.newBuilder()
                .column("orderId", DataTypes.BIGINT())
                .column("itemId", DataTypes.BIGINT())
                .column("amount", DataTypes.INT())
                .column("address", DataTypes.STRING())
                .primaryKey("orderId")
                .build();
    }

    public static Schema getOrdersSchemaLog() {
        return Schema.newBuilder()
                .column("orderId", DataTypes.BIGINT())
                .column("itemId", DataTypes.BIGINT())
                .column("amount", DataTypes.INT())
                .column("address", DataTypes.STRING())
                .build();
    }

    public static class OrderDeserializationSchema implements FlussDeserializationSchema<Order> {
        @Override
        public Order deserialize(ScanRecord scanRecord) throws Exception {
            InternalRow row = scanRecord.getRow();
            long orderId = row.getLong(0);
            long itemId = row.getLong(1);
            int amount = row.getInt(2);
            String address = String.valueOf(row.getString(3));
            return new Order(orderId, itemId, amount, address);
        }

        @Override
        public TypeInformation<Order> getProducedType() {
            return TypeInformation.of(Order.class);
        }
    }
}
