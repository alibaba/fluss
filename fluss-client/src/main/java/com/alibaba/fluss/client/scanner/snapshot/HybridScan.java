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

package com.alibaba.fluss.client.scanner.snapshot;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.fs.FsPathAndFileName;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.List;

/**
 * A class to describe hybrid scan for a single bucket, which include the snapshot files and a piece
 * of log.
 *
 * <p>It also contains the starting offset and ending offset comparing {@link SnapshotScan}
 *
 * @since 0.3
 */
@PublicEvolving
public class HybridScan extends SnapshotScan {
    private final long logStartingOffset;
    private final long logStoppingOffset;

    public HybridScan(
            TableBucket tableBucket,
            List<FsPathAndFileName> fsPathAndFileNames,
            Schema tableSchema,
            @Nullable int[] projectedFields,
            long logStartingOffset,
            long logStoppingOffset) {
        super(tableBucket, fsPathAndFileNames, tableSchema, projectedFields);
        this.logStartingOffset = logStartingOffset;
        this.logStoppingOffset = logStoppingOffset;
    }

    public long getLogStartingOffset() {
        return logStartingOffset;
    }

    public long getLogStoppingOffset() {
        return logStoppingOffset;
    }
}
