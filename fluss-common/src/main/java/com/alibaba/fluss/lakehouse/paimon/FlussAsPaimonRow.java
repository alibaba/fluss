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

package com.alibaba.fluss.lakehouse.paimon;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;

/**
 * A wrapper to wrap Fluss's {@link com.alibaba.fluss.row.InternalRow} to Paimon's {@link
 * InternalRow} .
 */
public class FlussAsPaimonRow implements InternalRow {

    private com.alibaba.fluss.row.InternalRow row;

    public void replace(com.alibaba.fluss.row.InternalRow row) {
        this.row = row;
    }

    @Override
    public int getFieldCount() {
        return row.getFieldCount();
    }

    @Override
    public RowKind getRowKind() {
        // don't create row kind, return insert directly
        return RowKind.INSERT;
    }

    @Override
    public void setRowKind(RowKind rowKind) {
        // do nothing
    }

    @Override
    public boolean isNullAt(int pos) {
        return row.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return row.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return row.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return row.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return row.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return row.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return row.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return row.getDouble(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromBytes(row.getString(pos).toBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        com.alibaba.fluss.row.Decimal decimal = row.getDecimal(pos, precision, scale);
        if (decimal.isCompact()) {
            return Decimal.fromUnscaledLong(decimal.toUnscaledLong(), precision, scale);
        } else {
            return Decimal.fromBigDecimal(decimal.toBigDecimal(), precision, scale);
        }
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return Timestamp.fromEpochMillis(row.getTimestampLtz(pos, precision).getEpochMillisecond());
    }

    @Override
    public byte[] getBinary(int pos) {
        return row.getBytes(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        throw new UnsupportedOperationException("getArray is not supported");
    }

    @Override
    public InternalMap getMap(int pos) {
        throw new UnsupportedOperationException("getMap is not supported");
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        throw new UnsupportedOperationException("getRow is not supported");
    }
}
