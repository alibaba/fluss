/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.server.kv.mergeengine;

import com.alibaba.fluss.config.MergeEngine;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.encode.ValueDecoder;
import com.alibaba.fluss.row.encode.ValueEncoder;
import com.alibaba.fluss.server.kv.partialupdate.PartialUpdater;
import com.alibaba.fluss.server.kv.prewrite.KvPreWriteBuffer;
import com.alibaba.fluss.server.kv.rocksdb.RocksDBKv;
import com.alibaba.fluss.server.kv.wal.WalBuilder;
import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.TimeType;
import com.alibaba.fluss.types.TimestampType;

/** A wrapper for version merge engine. */
public class VersionMergeEngineWrapper extends AbstractMergeEngineWrapper {

    private final MergeEngine mergeEngine;
    private final Schema schema;

    public VersionMergeEngineWrapper(
            PartialUpdater partialUpdater,
            ValueDecoder valueDecoder,
            WalBuilder walBuilder,
            KvPreWriteBuffer kvPreWriteBuffer,
            RocksDBKv rocksDBKv,
            Schema schema,
            short schemaId,
            int appendedRecordCount,
            long logOffset,
            MergeEngine mergeEngine) {
        super(
                partialUpdater,
                valueDecoder,
                walBuilder,
                kvPreWriteBuffer,
                rocksDBKv,
                schema,
                schemaId,
                appendedRecordCount,
                logOffset);
        this.mergeEngine = mergeEngine;
        this.schema = schema;
    }

    @Override
    protected void update(KvPreWriteBuffer.Key key, BinaryRow oldRow, BinaryRow newRow)
            throws Exception {
        RowType rowType = schema.toRowType();
        if (checkVersionMergeEngine(rowType, oldRow, newRow)) {
            return;
        }
        walBuilder.append(RowKind.UPDATE_BEFORE, oldRow);
        walBuilder.append(RowKind.UPDATE_AFTER, newRow);
        appendedRecordCount += 2;
        // logOffset is for -U, logOffset + 1 is for +U, we need to use
        // the log offset for +U
        kvPreWriteBuffer.put(key, ValueEncoder.encodeValue(schemaId, newRow), logOffset + 1);
        logOffset += 2;
    }

    private boolean checkVersionMergeEngine(RowType rowType, BinaryRow oldRow, BinaryRow newRow) {
        if (!checkNewRowVersion(mergeEngine, rowType, oldRow, newRow)) {
            // When the specified field version is less
            // than the version number of the old
            // record, do not update
            return true;
        }
        return false;
    }

    // Check row version.
    private boolean checkNewRowVersion(
            MergeEngine mergeEngine, RowType rowType, BinaryRow oldRow, BinaryRow newRow) {
        int fieldIndex = rowType.getFieldIndex(mergeEngine.getColumn());
        if (fieldIndex == -1) {
            return false;
        }
        DataType dataType = rowType.getTypeAt(fieldIndex);
        if (dataType instanceof BigIntType) {
            return newRow.getLong(fieldIndex) > oldRow.getLong(fieldIndex);
        } else if (dataType instanceof IntType) {
            return newRow.getInt(fieldIndex) > oldRow.getInt(fieldIndex);
        } else if (dataType instanceof TimestampType || dataType instanceof TimeType) {
            return newRow.getTimestampNtz(fieldIndex, ((TimestampType) dataType).getPrecision())
                            .getMillisecond()
                    > oldRow.getTimestampNtz(fieldIndex, ((TimestampType) dataType).getPrecision())
                            .getMillisecond();
        } else if (dataType instanceof LocalZonedTimestampType) {
            return newRow.getTimestampLtz(fieldIndex, ((TimestampType) dataType).getPrecision())
                            .toEpochMicros()
                    > oldRow.getTimestampLtz(fieldIndex, ((TimestampType) dataType).getPrecision())
                            .toEpochMicros();
        } else {
            throw new FlussRuntimeException("Unsupported data type: " + dataType.asSummaryString());
        }
    }
}
