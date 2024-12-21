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

import com.alibaba.fluss.client.scanner.RemoteFileDownloader;
import com.alibaba.fluss.client.scanner.log.LogScanner;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.KvFormat;

import javax.annotation.Nullable;

/** A hybrid scanner to merge snapshot and bounded log. */
public class HybridScanner extends SnapshotScanner {

    private final byte schemaId;
    private final LogScanner logScanner;
    private final int[] logProjectedFields;

    public HybridScanner(
            Configuration conf,
            KvFormat kvFormat,
            RemoteFileDownloader remoteFileDownloader,
            byte schemaId,
            HybridScan hybridScan,
            @Nullable int[] logProjectedFields,
            LogScanner logScanner) {
        super(conf, kvFormat, remoteFileDownloader, hybridScan);
        this.logScanner = logScanner;
        this.schemaId = schemaId;
        this.logProjectedFields = logProjectedFields;
    }

    /**
     * Override initReader to provide {@link HybridFilesReader} and subscribe and apply a piece of
     * logs.
     */
    @Override
    protected SnapshotFilesReader createSnapshotReader() {
        return new HybridFilesReader(
                kvFormat,
                snapshotLocalDirectory,
                schemaId,
                snapshotScan.getTableSchema(),
                snapshotScan.getProjectedFields(),
                logProjectedFields,
                ((HybridScan) snapshotScan).getLogStartingOffset(),
                ((HybridScan) snapshotScan).getLogStoppingOffset(),
                snapshotScan.getTableBucket(),
                logScanner);
    }
}
