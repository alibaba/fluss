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

import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.record.KvRecord;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.encode.ValueDecoder;
import com.alibaba.fluss.row.encode.ValueEncoder;
import com.alibaba.fluss.server.kv.partialupdate.PartialUpdater;
import com.alibaba.fluss.server.kv.prewrite.KvPreWriteBuffer;
import com.alibaba.fluss.server.kv.rocksdb.RocksDBKv;
import com.alibaba.fluss.server.kv.wal.WalBuilder;
import com.alibaba.fluss.utils.BytesUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/** Abstract common operations for merge engine wrapper. */
public abstract class AbstractMergeEngineWrapper implements MergeEngineWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMergeEngineWrapper.class);
    protected final PartialUpdater partialUpdater;
    protected final ValueDecoder valueDecoder;
    protected final WalBuilder walBuilder;
    protected final KvPreWriteBuffer kvPreWriteBuffer;
    protected final RocksDBKv rocksDBKv;
    protected final Schema schema;
    protected final short schemaId;
    protected int appendedRecordCount;
    protected long logOffset;

    public AbstractMergeEngineWrapper(
            PartialUpdater partialUpdater,
            ValueDecoder valueDecoder,
            WalBuilder walBuilder,
            KvPreWriteBuffer kvPreWriteBuffer,
            RocksDBKv rocksDBKv,
            Schema schema,
            short schemaId,
            int appendedRecordCount,
            long logOffset) {
        this.partialUpdater = partialUpdater;
        this.valueDecoder = valueDecoder;
        this.walBuilder = walBuilder;
        this.kvPreWriteBuffer = kvPreWriteBuffer;
        this.rocksDBKv = rocksDBKv;
        this.schema = schema;
        this.schemaId = schemaId;
        this.appendedRecordCount = appendedRecordCount;
        this.logOffset = logOffset;
    }

    @Override
    public int getAppendedRecordCount() {
        return appendedRecordCount;
    }

    @Override
    public long getLogOffset() {
        return logOffset;
    }

    @Override
    public void writeRecord(KvRecord kvRecord) throws Exception {
        byte[] keyBytes = BytesUtils.toArray(kvRecord.getKey());
        KvPreWriteBuffer.Key key = KvPreWriteBuffer.Key.of(keyBytes);
        if (kvRecord.getRow() == null) {
            // it's for deletion
            byte[] oldValue = getFromBufferOrKv(key);
            if (oldValue == null) {
                // there might be large amount of such deletion, so we don't log
                LOG.debug(
                        "The specific key can't be found in kv tablet although the kv record is for deletion, "
                                + "ignore it directly as it doesn't exist in the kv tablet yet.");
            } else {
                BinaryRow oldRow = valueDecoder.decodeValue(oldValue).row;
                BinaryRow newRow = deleteRow(oldRow, partialUpdater);
                // if newRow is null, it means the row should be deleted
                if (newRow == null) {
                    delete(key, oldRow);
                } else {
                    // otherwise, it's a partial update, should produce -U,+U
                    update(key, oldRow, newRow);
                }
            }
        } else {
            // upsert operation
            byte[] oldValue = getFromBufferOrKv(key);
            // it's update
            if (oldValue != null) {
                BinaryRow oldRow = valueDecoder.decodeValue(oldValue).row;
                BinaryRow newRow = updateRow(oldRow, kvRecord.getRow(), partialUpdater);
                update(key, oldRow, newRow);
            } else {
                // it's insert
                // TODO: we should add guarantees that all non-specified columns
                //  of the input row are set to null.
                insert(key, kvRecord);
            }
        }
    }

    protected void delete(KvPreWriteBuffer.Key key, BinaryRow oldRow) throws Exception {
        walBuilder.append(RowKind.DELETE, oldRow);
        appendedRecordCount += 1;
        kvPreWriteBuffer.delete(key, logOffset++);
    }

    protected void insert(KvPreWriteBuffer.Key key, KvRecord kvRecord) throws Exception {
        BinaryRow newRow = kvRecord.getRow();
        walBuilder.append(RowKind.INSERT, newRow);
        appendedRecordCount += 1;
        kvPreWriteBuffer.put(key, ValueEncoder.encodeValue(schemaId, newRow), logOffset++);
    }

    protected void update(KvPreWriteBuffer.Key key, BinaryRow oldRow, BinaryRow newRow)
            throws Exception {
        walBuilder.append(RowKind.UPDATE_BEFORE, oldRow);
        walBuilder.append(RowKind.UPDATE_AFTER, newRow);
        appendedRecordCount += 2;
        // logOffset is for -U, logOffset + 1 is for +U, we need to use
        // the log offset for +U
        kvPreWriteBuffer.put(key, ValueEncoder.encodeValue(schemaId, newRow), logOffset + 1);
        logOffset += 2;
    }

    // get from kv pre-write buffer first, if can't find, get from rocksdb
    protected byte[] getFromBufferOrKv(KvPreWriteBuffer.Key key) throws IOException {
        KvPreWriteBuffer.Value value = kvPreWriteBuffer.get(key);
        if (value == null) {
            return rocksDBKv.get(key.get());
        }
        return value.get();
    }

    protected @Nullable BinaryRow deleteRow(
            InternalRow oldRow, @Nullable PartialUpdater partialUpdater) {
        if (partialUpdater == null) {
            return null;
        }
        return partialUpdater.deleteRow(oldRow);
    }

    protected BinaryRow updateRow(
            BinaryRow oldRow, BinaryRow updateRow, @Nullable PartialUpdater partialUpdater) {
        // if is not partial update, return the update row
        if (partialUpdater == null) {
            return updateRow;
        }
        // otherwise, do partial update
        return partialUpdater.updateRow(oldRow, updateRow);
    }
}
