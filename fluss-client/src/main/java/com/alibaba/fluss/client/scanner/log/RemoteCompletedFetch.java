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

package com.alibaba.fluss.client.scanner.log;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.FileLogRecords;
import com.alibaba.fluss.record.LogRecordReadContext;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.InternalRow.FieldGetter;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.Projection;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * {@link RemoteCompletedFetch} is a {@link CompletedFetch} that represents a completed fetch that
 * the log records are fetched from remote log storage.
 */
@Internal
class RemoteCompletedFetch extends CompletedFetch {

    private final FileLogRecords fileLogRecords;

    // recycle to clean up the fetched remote log files and increment the prefetch semaphore
    private final Runnable recycleCallback;

    RemoteCompletedFetch(
            TableBucket tableBucket,
            FileLogRecords fileLogRecords,
            long highWatermark,
            LogRecordReadContext readContext,
            LogScannerStatus logScannerStatus,
            boolean isCheckCrc,
            long fetchOffset,
            @Nullable Projection projection,
            Runnable recycleCallback) {
        super(
                tableBucket,
                ApiError.NONE,
                fileLogRecords.sizeInBytes(),
                highWatermark,
                fileLogRecords.batches().iterator(),
                readContext,
                logScannerStatus,
                isCheckCrc,
                fetchOffset,
                projection);
        this.fileLogRecords = fileLogRecords;
        this.recycleCallback = recycleCallback;
    }

    @Override
    void drain() {
        super.drain();
        // close file channel only, don't need to flush the file which is very heavy
        try {
            fileLogRecords.closeHandlers();
        } catch (IOException e) {
            LOG.warn("Failed to close file channel for remote log records", e);
        }
        // call recycle to remove the fetched files and increment the prefetch semaphore
        recycleCallback.run();
    }

    @Override
    protected FieldGetter[] buildFieldGetters() {
        RowType rowType = readContext.getRowType();
        FieldGetter[] fieldGetters;
        if (projection != null) {
            int[] projectionInOrder = projection.getProjectionInOrder();
            fieldGetters = new FieldGetter[projectionInOrder.length];
            for (int i = 0; i < fieldGetters.length; i++) {
                fieldGetters[i] =
                        InternalRow.createFieldGetter(
                                rowType.getChildren().get(projectionInOrder[i]),
                                projectionInOrder[i]);
            }
        } else {
            fieldGetters = new FieldGetter[rowType.getChildren().size()];
            for (int i = 0; i < fieldGetters.length; i++) {
                fieldGetters[i] = InternalRow.createFieldGetter(rowType.getChildren().get(i), i);
            }
        }

        return fieldGetters;
    }
}
