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

package com.alibaba.fluss.lakehouse.paimon.source.reader;

import com.alibaba.fluss.lakehouse.paimon.source.metrics.FlinkSourceReaderMetrics;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.utils.CloseableIterator;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** An implementation of {@link RecordsWithSplitIds} which contains records from multiple splits. */
public class FlinkRecordsWithSplitIds implements RecordsWithSplitIds<MultiplexCdcRecordAndPos> {

    /** The finished splits. */
    private final Set<String> finishedSplits;

    /** SplitId -> records. */
    private final Map<String, CloseableIterator<MultiplexCdcRecordAndPos>> splitRecords;

    /** SplitId iterator. */
    private final Iterator<String> splitIterator;
    /** The table buckets of the split in splitIterator. */
    private final Iterator<TableBucket> tableBucketIterator;

    private final FlinkSourceReaderMetrics flinkSourceReaderMetrics;

    // the closable iterator for the records in the current split
    private @Nullable CloseableIterator<MultiplexCdcRecordAndPos> currentRecordIterator;
    private @Nullable TableBucket currentTableBucket;

    // for multiple splits
    public FlinkRecordsWithSplitIds(
            Map<String, CloseableIterator<MultiplexCdcRecordAndPos>> splitRecords,
            Iterator<String> splitIterator,
            Iterator<TableBucket> tableBucketIterator,
            FlinkSourceReaderMetrics flinkSourceReaderMetrics) {
        this(
                splitRecords,
                splitIterator,
                tableBucketIterator,
                new HashSet<>(),
                flinkSourceReaderMetrics);
    }

    // only for single split
    public FlinkRecordsWithSplitIds(
            String split,
            TableBucket tableBucket,
            CloseableIterator<MultiplexCdcRecordAndPos> records,
            FlinkSourceReaderMetrics flinkSourceReaderMetrics) {
        this(
                Collections.singletonMap(split, records),
                Collections.singleton(split).iterator(),
                Collections.singleton(tableBucket).iterator(),
                new HashSet<>(),
                flinkSourceReaderMetrics);
    }

    // no any splits, just used to mark splits finished
    public FlinkRecordsWithSplitIds(
            Set<String> finishedSplits, FlinkSourceReaderMetrics flinkSourceReaderMetrics) {
        this(
                Collections.emptyMap(),
                Collections.emptyIterator(),
                Collections.emptyIterator(),
                finishedSplits,
                flinkSourceReaderMetrics);
    }

    private FlinkRecordsWithSplitIds(
            Map<String, CloseableIterator<MultiplexCdcRecordAndPos>> splitRecords,
            Iterator<String> splitIterator,
            Iterator<TableBucket> tableBucketIterator,
            Set<String> finishedSplits,
            FlinkSourceReaderMetrics flinkSourceReaderMetrics) {
        this.splitRecords = splitRecords;
        this.splitIterator = splitIterator;
        this.tableBucketIterator = tableBucketIterator;
        this.finishedSplits = finishedSplits;
        this.flinkSourceReaderMetrics = flinkSourceReaderMetrics;
    }

    @Nullable
    @Override
    public String nextSplit() {
        if (splitIterator.hasNext()) {
            String currentSplit = splitIterator.next();
            currentRecordIterator = splitRecords.get(currentSplit);
            currentTableBucket = tableBucketIterator.next();
            return currentSplit;
        } else {
            currentRecordIterator = null;
            currentTableBucket = null;
            return null;
        }
    }

    @Nullable
    @Override
    public MultiplexCdcRecordAndPos nextRecordFromSplit() {
        checkNotNull(
                currentRecordIterator,
                "Make sure nextSplit() did not return null before "
                        + "iterate over the records split.");
        if (currentRecordIterator.hasNext()) {
            MultiplexCdcRecordAndPos recordAndPos = currentRecordIterator.next();
            if (recordAndPos.record().getOffset() > 0) {
                flinkSourceReaderMetrics.recordCurrentOffset(
                        currentTableBucket, recordAndPos.record().getOffset());
            }
            return recordAndPos;
        }
        return null;
    }

    @Override
    public void recycle() {
        // close records iterator for all splits
        splitRecords.values().forEach(CloseableIterator::close);
    }

    @Override
    public Set<String> finishedSplits() {
        return finishedSplits;
    }
}
