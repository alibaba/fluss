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

package com.alibaba.fluss.flink.sink.serializer;

import com.alibaba.fluss.flink.row.FlinkAsFlussRow;
import com.alibaba.fluss.flink.row.RowWithOp;
import com.alibaba.fluss.row.InternalRow;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

/** Default implementation of RowDataConverter for RowData. */
public class RowSerializationSchema implements FlussSerializationSchema<RowData> {
    private FlinkAsFlussRow converter;

    @Override
    public void open(InitializationContext context) throws Exception {
        this.converter = new FlinkAsFlussRow();
    }

    @Override
    public RowWithOp<RowData> serialize(RowData value) throws Exception {
        if (this.converter == null) {
            this.converter = new FlinkAsFlussRow();
        }

        InternalRow row = converter.replace(value);
        RowWithOp<RowData> rowWithOp = new RowWithOp<>(row);
        switch (value.getRowKind()) {
            case INSERT:
                rowWithOp.setRowKind(RowKind.INSERT);
                return rowWithOp;
            case UPDATE_BEFORE:
                rowWithOp.setRowKind(RowKind.UPDATE_BEFORE);
                return rowWithOp;
            case UPDATE_AFTER:
                rowWithOp.setRowKind(RowKind.UPDATE_AFTER);
                return rowWithOp;
            case DELETE:
                rowWithOp.setRowKind(RowKind.DELETE);
                return rowWithOp;
            default:
                throw new TableException("Unsupported message kind: " + value.getRowKind());
        }
    }
}
