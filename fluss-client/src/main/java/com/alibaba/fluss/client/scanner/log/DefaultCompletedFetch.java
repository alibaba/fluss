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
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.LogRecordReadContext;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.InternalRow.FieldGetter;
import com.alibaba.fluss.rpc.entity.FetchLogResultForBucket;
import com.alibaba.fluss.rpc.messages.FetchLogRequest;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.Projection;

import javax.annotation.Nullable;

/**
 * {@link DefaultCompletedFetch} is a {@link CompletedFetch} that represents a completed fetch that
 * the log records have been returned from the tablet server by {@link FetchLogRequest}.
 */
@Internal
class DefaultCompletedFetch extends CompletedFetch {

    public DefaultCompletedFetch(
            TableBucket tableBucket,
            FetchLogResultForBucket fetchLogResultForBucket,
            LogRecordReadContext readContext,
            LogScannerStatus logScannerStatus,
            boolean isCheckCrc,
            Long fetchOffset,
            @Nullable Projection projection) {
        super(
                tableBucket,
                fetchLogResultForBucket.getError(),
                fetchLogResultForBucket.recordsOrEmpty().sizeInBytes(),
                fetchLogResultForBucket.getHighWatermark(),
                fetchLogResultForBucket.recordsOrEmpty().batches().iterator(),
                readContext,
                logScannerStatus,
                isCheckCrc,
                fetchOffset,
                projection);
    }

    @Override
    protected FieldGetter[] buildFieldGetters() {
        RowType rowType = readContext.getRowType();
        LogFormat logFormat = readContext.getLogFormat();
        FieldGetter[] fieldGetters;
        // Arrow log format already project in the server side.
        if (projection != null && logFormat != LogFormat.ARROW) {
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
