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

package com.alibaba.fluss.flink.helper;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.flink.serdes.FlussDeserializationSchema;
import com.alibaba.fluss.row.InternalRow;

import org.apache.flink.api.common.typeinfo.TypeInformation;

public class OrderPartialDeserializationSchema implements FlussDeserializationSchema<OrderPartial> {

    @Override
    public OrderPartial deserialize(ScanRecord scanRecord) throws Exception {
        InternalRow row = scanRecord.getRow();

        long orderId = row.getLong(0);
        int amount = row.getInt(1);

        return new OrderPartial(orderId, amount);
    }

    @Override
    public TypeInformation<OrderPartial> getProducedType() {
        return TypeInformation.of(OrderPartial.class);
    }
}
