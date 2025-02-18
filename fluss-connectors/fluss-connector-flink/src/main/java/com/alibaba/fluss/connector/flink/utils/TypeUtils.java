/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.connector.flink.utils;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.TimestampType;
import com.alibaba.fluss.utils.BinaryStringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Type related helper functions. */
public class TypeUtils {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(TypeUtils.class);

    public static RowType concat(RowType left, RowType right) {
        RowType.Builder builder = RowType.builder();
        List<DataField> fields = new ArrayList<>(left.getFields());
        fields.addAll(right.getFields());
        fields.forEach(
                dataField ->
                        builder.field(
                                dataField.getName(),
                                dataField.getType(),
                                dataField.getDescription().get()));
        return builder.build();
    }

    public static RowType project(RowType inputType, int[] mapping) {
        List<DataField> fields = inputType.getFields();
        return new RowType(
                Arrays.stream(mapping).mapToObj(fields::get).collect(Collectors.toList()));
    }

    public static RowType project(RowType inputType, List<String> names) {
        List<DataField> fields = inputType.getFields();
        List<String> fieldNames =
                fields.stream().map(DataField::getName).collect(Collectors.toList());
        return new RowType(
                names.stream()
                        .map(k -> fields.get(fieldNames.indexOf(k)))
                        .collect(Collectors.toList()));
    }

    public static Object castFromString(String s, DataType type) {
        return castFromStringInternal(s, type, false);
    }

    public static Object castFromCdcValueString(String s, DataType type) {
        return castFromStringInternal(s, type, true);
    }

    public static Object castFromStringInternal(String s, DataType type, boolean isCdcValue) {
        BinaryString str = BinaryString.fromString(s);
        switch (type.getTypeRoot()) {
            case CHAR:
            case STRING:
                return s;
            case BOOLEAN:
                return BinaryStringUtils.toBoolean(str);
            case BINARY:
                return isCdcValue
                        ? Base64.getDecoder().decode(s)
                        : s.getBytes(StandardCharsets.UTF_8);

            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return Decimal.fromBigDecimal(
                        new BigDecimal(s), decimalType.getPrecision(), decimalType.getScale());
            case TINYINT:
                return Byte.valueOf(s);
            case SMALLINT:
                return Short.valueOf(s);
            case INTEGER:
                return Integer.valueOf(s);
            case BIGINT:
                return Long.valueOf(s);
            case FLOAT:
                double d = Double.parseDouble(s);
                if (d == ((float) d)) {
                    return (float) d;
                } else {
                    // Compatible canal-cdc
                    Float f = Float.valueOf(s);
                    if (f.toString().length() != s.length()) {
                        throw new NumberFormatException(
                                s + " cannot be cast to float due to precision loss");
                    } else {
                        return f;
                    }
                }
            case DOUBLE:
                return Double.valueOf(s);
            case DATE:
                return BinaryStringUtils.toDate(str);
            case TIME_WITHOUT_TIME_ZONE:
                return BinaryStringUtils.toTime(str);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) type;
                return BinaryStringUtils.toTimestampNtz(str, timestampType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedTimestampType = (LocalZonedTimestampType) type;
                return BinaryStringUtils.toTimestampltz(
                        str, localZonedTimestampType.getPrecision(), TimeZone.getDefault());

            default:
                throw new UnsupportedOperationException("Unsupported type " + type);
        }
    }

    public static boolean isBasicType(Object obj) {
        Class<?> clazz = obj.getClass();
        return clazz.isPrimitive() || isWrapperType(clazz) || clazz.equals(String.class);
    }

    private static boolean isWrapperType(Class<?> clazz) {
        return clazz.equals(Boolean.class)
                || clazz.equals(Character.class)
                || clazz.equals(Byte.class)
                || clazz.equals(Short.class)
                || clazz.equals(Integer.class)
                || clazz.equals(Long.class)
                || clazz.equals(Float.class)
                || clazz.equals(Double.class);
    }
}
