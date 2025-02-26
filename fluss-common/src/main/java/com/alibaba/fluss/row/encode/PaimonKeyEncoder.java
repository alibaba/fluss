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

package com.alibaba.fluss.row.encode;

import com.alibaba.fluss.lakehouse.paimon.FlussAsPaimonRow;
import com.alibaba.fluss.lakehouse.paimon.FlussDataTypeToPaimonDataType;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.ProjectedRow;
import com.alibaba.fluss.types.RowType;

import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.DataType;

import java.util.List;

/** An implementation of {@link KeyEncoder} to follow Paimon's encoding strategy. */
public class PaimonKeyEncoder implements KeyEncoder {

    private final FlussAsPaimonRow paimonBucketRow;
    private final InternalRowSerializer bucketKeyRowSerializer;
    private final ProjectedRow bucketKeyProjectedRow;

    public PaimonKeyEncoder(RowType rowType, List<String> keys) {
        int[] bucketKeyIndex = getBucketKeyIndex(rowType, keys);
        this.bucketKeyProjectedRow = ProjectedRow.from(bucketKeyIndex);
        DataType[] bucketKeyDataTypes =
                rowType.project(bucketKeyIndex).getChildren().stream()
                        .map(dataType -> dataType.accept(FlussDataTypeToPaimonDataType.INSTANCE))
                        .toArray(DataType[]::new);
        this.bucketKeyRowSerializer = new InternalRowSerializer(bucketKeyDataTypes);
        this.paimonBucketRow = new FlussAsPaimonRow();
    }

    private int[] getBucketKeyIndex(RowType rowType, List<String> bucketKey) {
        int[] bucketKeyIndex = new int[bucketKey.size()];
        for (int i = 0; i < bucketKey.size(); i++) {
            bucketKeyIndex[i] = rowType.getFieldIndex(bucketKey.get(i));
        }
        return bucketKeyIndex;
    }

    @Override
    public byte[] encodeKey(InternalRow row) {
        // todo: remove paimon dependency in #408
        InternalRow bucketRow = bucketKeyProjectedRow.replaceRow(row);
        // wrap to paimon's InternalRow
        paimonBucketRow.replace(bucketRow);
        return bucketKeyRowSerializer.toBinaryRow(paimonBucketRow).toBytes();
    }
}
