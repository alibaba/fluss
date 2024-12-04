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

package com.alibaba.fluss.lakehouse.paimon.source.reader;

import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.client.scanner.snapshot.SnapshotScanner;
import com.alibaba.fluss.lakehouse.paimon.record.CdcRecord;
import com.alibaba.fluss.lakehouse.paimon.source.utils.FlussRowToFlinkRowConverter;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.utils.CloseableIterator;

import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Iterator;

/**
 * A snapshot reader to reading Fluss's snapshot data into {@link MultiplexCdcRecordAndPos}s.
 *
 * <p>It wraps a {@link SnapshotScanner} to read snapshot data, skips the {@link #toSkip} records
 * while reading and produce {@link MultiplexCdcRecordAndPos}s with the current reading records
 * count.
 *
 * <p>In method {@link #readBatch()}, it'll first skip the {@link #toSkip} records, and then return
 * the {@link MultiplexCdcRecordAndPos}s.
 */
class SnapshotReader implements AutoCloseable {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(10000L);
    private static final int BATCH_SIZE = 1024;

    private final TablePath tablePath;
    private final TableBucket tableBucket;
    private final SnapshotScanner snapshotScanner;
    private final FlussRowToFlinkRowConverter converter;
    private long currentReadRecordsCount;
    private long toSkip;

    public SnapshotReader(
            TablePath tablePath,
            TableBucket tableBucket,
            SnapshotScanner snapshotScanner,
            FlussRowToFlinkRowConverter converter,
            final long toSkip) {
        this.tablePath = tablePath;
        this.tableBucket = tableBucket;
        this.snapshotScanner = snapshotScanner;
        this.converter = converter;
        this.toSkip = toSkip;
        this.currentReadRecordsCount = 0;
    }

    /** Read next batch of data. Return null when no data is available. */
    @Nullable
    CloseableIterator<MultiplexCdcRecordAndPos> readBatch() {
        Iterator<ScanRecord> recordIterator = poll();
        return recordIterator == null ? null : new RecordAndPosBatch(recordIterator);
    }

    private Iterator<ScanRecord> poll() {
        Iterator<ScanRecord> nextBatch = null;
        // may skip records
        while (toSkip > 0) {
            // pool a batch of records
            nextBatch = pollBatch();
            // no more records, but still need to skip records
            if (nextBatch == null) {
                throw new RuntimeException(
                        String.format(
                                "Skip more than the number of total records, has skipped %d record(s), but remain %s record(s) to skip.",
                                currentReadRecordsCount, toSkip));
            }
            // skip
            while (toSkip > 0 && nextBatch.hasNext()) {
                nextBatch.next();
                toSkip--;
                currentReadRecordsCount++;
            }
        }
        // if any batch remains while skipping, return the batch
        if (nextBatch == null || !nextBatch.hasNext()) {
            // otherwise pool next batch
            nextBatch = pollBatch();
            // return null if the new batch has no more records
        }
        return nextBatch;
    }

    @Override
    public void close() {
        snapshotScanner.close();
    }

    private Iterator<ScanRecord> pollBatch() {
        // reach end, return null directly
        if (snapshotScanner.reachedEnd()) {
            return null;
        }
        Iterator<ScanRecord> records = snapshotScanner.poll(POLL_TIMEOUT);
        return records == null ? null : new ScanRecordBatch(records);
    }

    private static class ScanRecordBatch implements Iterator<ScanRecord> {
        private int currentRecords = 0;
        private final Iterator<ScanRecord> scanRecordIterator;

        public ScanRecordBatch(Iterator<ScanRecord> scanRecordIterator) {
            this.scanRecordIterator = scanRecordIterator;
        }

        @Override
        public boolean hasNext() {
            return scanRecordIterator.hasNext() && currentRecords < BATCH_SIZE;
        }

        @Override
        public ScanRecord next() {
            currentRecords++;
            return scanRecordIterator.next();
        }
    }

    private class RecordAndPosBatch implements CloseableIterator<MultiplexCdcRecordAndPos> {
        private final Iterator<ScanRecord> records;

        RecordAndPosBatch(Iterator<ScanRecord> records) {
            this.records = records;
        }

        @Override
        public boolean hasNext() {
            return records.hasNext();
        }

        @Override
        public MultiplexCdcRecordAndPos next() {
            // todo: in here, we always create a new RecordAndPos object,
            // may use a mutable record and pos class like Flink's MutableRecordAndPosition
            return toMultiplexCdcRecordAndPos(
                    tablePath, tableBucket, converter, records.next(), ++currentReadRecordsCount);
        }

        @Override
        public void close() {
            // we have reached end of the snapshot scanner, close it
            if (snapshotScanner.reachedEnd()) {
                snapshotScanner.close();
            }
        }
    }

    private MultiplexCdcRecordAndPos toMultiplexCdcRecordAndPos(
            TablePath tablePath,
            TableBucket tableBucket,
            FlussRowToFlinkRowConverter converter,
            ScanRecord scanRecord,
            long pos) {
        RowData rowData = converter.toFlinkRowData(scanRecord);
        CdcRecord cdcRecord =
                new CdcRecord(scanRecord.getOffset(), scanRecord.getTimestamp(), rowData);
        return new MultiplexCdcRecordAndPos(tablePath, tableBucket, cdcRecord, pos);
    }
}
