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

package com.alibaba.fluss.flink.row;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.TimestampNtz;

import org.apache.flink.table.types.logical.LogicalType;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utility class for converting between different row representations in Fluss.
 *
 * <p>It's primarily used for testing and data conversion operations when interacting with the Fluss
 * storage system.
 */
public class RowConverters {
    /**
     * Converts a POJO to a Fluss GenericRow. Special handling is provided for timestamp types.
     *
     * @param pojo The POJO to convert
     * @return A GenericRow containing the POJO's field values
     * @throws IllegalArgumentException if there's an error accessing the fields
     */
    public static GenericRow pojoToGenericRow(Object pojo) {
        if (pojo == null) {
            throw new IllegalArgumentException("Input POJO cannot be null");
        }

        Field[] allFields = pojo.getClass().getDeclaredFields();

        // Filter out synthetic fields (JaCoCo's $jacocoData)
        Field[] fields =
                Arrays.stream(allFields)
                        .filter(
                                field ->
                                        !field.isSynthetic()
                                                && !Modifier.isStatic(field.getModifiers()))
                        .toArray(Field[]::new);

        Object[] values = new Object[fields.length];

        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);
            try {
                Object value = field.get(pojo);

                // Handle various field types
                if (value instanceof String) {
                    values[i] = BinaryString.fromString((String) value);
                } else if (value instanceof LocalDateTime) {
                    values[i] = TimestampNtz.fromLocalDateTime((LocalDateTime) value);
                } else if (value instanceof Date) {
                    values[i] = TimestampNtz.fromMillis(((Date) value).getTime());
                } else if (value instanceof Instant) {
                    LocalDateTime ldt =
                            LocalDateTime.ofInstant((Instant) value, ZoneId.systemDefault());
                    values[i] = TimestampNtz.fromLocalDateTime(ldt);
                } else {
                    values[i] = value;
                }
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Unable to access field: " + field.getName(), e);
            }
        }

        return row(values);
    }

    public static GenericRow row(Object... objects) {
        GenericRow row = new GenericRow(objects.length);
        for (int i = 0; i < objects.length; i++) {
            if (objects[i] instanceof String) {
                row.setField(i, BinaryString.fromString((String) objects[i]));
            } else {
                row.setField(i, objects[i]);
            }
        }
        return row;
    }

    /**
     * Converts a Fluss RowType to a Flink RowType.
     *
     * @param flussRowType The Fluss RowType to convert
     * @return The equivalent Flink RowType
     */
    public static org.apache.flink.table.types.logical.RowType flussRowTypeToFlinkRowType(
            com.alibaba.fluss.types.RowType flussRowType) {

        List<LogicalType> flinkFieldTypes = new ArrayList<>();
        List<String> flinkFieldNames = new ArrayList<>();

        int fieldCount = flussRowType.getFieldCount();

        for (int i = 0; i < fieldCount; i++) {
            flinkFieldNames.add(flussRowType.getFieldNames().get(i));
            flinkFieldTypes.add(convertFlussTypeToFlinkType(flussRowType.getTypeAt(i)));
        }

        return org.apache.flink.table.types.logical.RowType.of(
                flinkFieldTypes.toArray(new LogicalType[0]),
                flinkFieldNames.toArray(new String[0]));
    }

    /**
     * Converts a Fluss DataType to a Flink LogicalType.
     *
     * @param flussType The Fluss DataType to convert
     * @return The equivalent Flink LogicalType
     */
    private static org.apache.flink.table.types.logical.LogicalType convertFlussTypeToFlinkType(
            com.alibaba.fluss.types.DataType flussType) {
        // Use the correct method to get the type name
        String typeName = flussType.toString().toUpperCase();

        // Check for nullability
        boolean isNullable = !typeName.contains("NOT NULL");

        // Remove NOT NULL for processing the base type
        if (!isNullable) {
            typeName = typeName.replace("NOT NULL", "").trim();
        }

        // In Flink, we need to specify nullability when creating the type
        switch (typeName) {
            case "BOOLEAN":
                return new org.apache.flink.table.types.logical.BooleanType(isNullable);
            case "BIGINT":
            case "LONG":
                return new org.apache.flink.table.types.logical.BigIntType(isNullable);
            case "INTEGER":
            case "INT":
                return new org.apache.flink.table.types.logical.IntType(isNullable);
            case "FLOAT":
                return new org.apache.flink.table.types.logical.FloatType(isNullable);
            case "DOUBLE":
                return new org.apache.flink.table.types.logical.DoubleType(isNullable);
            case "STRING":
            case "VARCHAR":
                return new org.apache.flink.table.types.logical.VarCharType(
                        isNullable, org.apache.flink.table.types.logical.VarCharType.MAX_LENGTH);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + flussType);
        }
    }
}
