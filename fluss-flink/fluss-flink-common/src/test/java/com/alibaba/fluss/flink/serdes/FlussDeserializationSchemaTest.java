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
import com.alibaba.fluss.flink.helper.Order;
import com.alibaba.fluss.flink.helper.OrderDeserializationSchema;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class FlussDeserializationSchemaTest {
    @Test
    public void testDeserialize() throws Exception {
        // Create GenericRow with proper types
        GenericRow row = new GenericRow(4);
        row.setField(0, 1001L);
        row.setField(1, 5001L);
        row.setField(2, 3);
        row.setField(3, BinaryString.fromString("123 Main St"));

        ScanRecord scanRecord = new ScanRecord(row);
        OrderDeserializationSchema schema = new OrderDeserializationSchema();

        Order result = schema.deserialize(scanRecord);

        assertEquals(1001L, result.getOrderId());
        assertEquals(5001L, result.getItemId());
        assertEquals(3, result.getAmount());
        assertEquals("123 Main St", result.getAddress());
    }

    @Test
    public void testDeserializeWithNumericConversion() throws Exception {
        GenericRow row = new GenericRow(4);
        row.setField(0, 1002L);
        row.setField(1, 5002L);
        row.setField(2, 4);
        row.setField(3, BinaryString.fromString("456 Oak Ave"));

        ScanRecord scanRecord = new ScanRecord(row);
        OrderDeserializationSchema schema = new OrderDeserializationSchema();

        Order result = schema.deserialize(scanRecord);

        assertEquals(1002L, result.getOrderId());
        assertEquals(5002L, result.getItemId());
        assertEquals(4, result.getAmount());
        assertEquals("456 Oak Ave", result.getAddress());
    }

    @Test
    public void testDeserializeWithNullValues() throws Exception {
        GenericRow row = new GenericRow(4);
        row.setField(0, 1003L);
        row.setField(1, 5003L);
        row.setField(2, 5);
        row.setField(3, null);

        ScanRecord scanRecord = new ScanRecord(row);
        OrderDeserializationSchema schema = new OrderDeserializationSchema();

        Order result = schema.deserialize(scanRecord);

        assertEquals(1003L, result.getOrderId());
        assertEquals(5003L, result.getItemId());
        assertEquals(5, result.getAmount());
        assertEquals("null", result.getAddress());
    }

    @Test
    public void testDeserializeWithNullRecord() {
        OrderDeserializationSchema schema = new OrderDeserializationSchema();

        Exception exception =
                assertThrows(
                        NullPointerException.class,
                        () -> {
                            schema.deserialize(null);
                        });

        assertNotNull(exception);
    }

    @Test
    public void testGetProducedType() {
        OrderDeserializationSchema schema = new OrderDeserializationSchema();
        TypeInformation<Order> typeInfo = schema.getProducedType();

        assertNotNull(typeInfo);
        assertEquals(Order.class, typeInfo.getTypeClass());
    }

    @Test
    public void testSerializable() throws Exception {
        OrderDeserializationSchema schema = new OrderDeserializationSchema();

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(schema);
        oos.close();

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        OrderDeserializationSchema deserializedSchema =
                (OrderDeserializationSchema) ois.readObject();
        ois.close();

        assertNotNull(deserializedSchema);
        assertEquals(schema.getProducedType(), deserializedSchema.getProducedType());
    }
}
