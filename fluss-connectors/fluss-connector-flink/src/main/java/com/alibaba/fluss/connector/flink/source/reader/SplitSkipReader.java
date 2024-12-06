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

package com.alibaba.fluss.connector.flink.source.reader;

import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.alibaba.fluss.connector.flink.source.reader.RecordAndPos.NO_READ_RECORDS_COUNT;

/**
 * A skip reader to reading Fluss's split into {@link RecordAndPos}s.
 *
 * <p>It wraps a {@link SplitScanner} to read data, skips the {@link #toSkip} records while reading
 * and produce {@link RecordAndPos}s with the current reading records count.
 *
 * <p>In method {@link #readBatch()}, it'll first skip the {@link #toSkip} records, and then return
 * the {@link RecordAndPos}s.
 */
public class SplitSkipReader implements AutoCloseable {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(10000L);

    private final SplitScanner splitScanner;
    private long currentReadRecordsCount;
    private long toSkip;

    private final BlockingQueue<RecordAndPosBatch> recordAndPosBatchPool;

    public SplitSkipReader(SplitScanner splitScanner, final long toSkip) {
        this.splitScanner = splitScanner;
        this.toSkip = toSkip;
        this.currentReadRecordsCount = 0;
        this.recordAndPosBatchPool = new ArrayBlockingQueue<>(1);
        this.recordAndPosBatchPool.add(new RecordAndPosBatch());
    }

    /** Read next batch of data. Return null when no data is available. */
    @Nullable
    CloseableIterator<RecordAndPos> readBatch() throws IOException {
        // pool a RecordAndPosBatch, pool size is 1, the underlying implementation does not allow
        // multiple batches to be read at the same time
        RecordAndPosBatch recordAndPosBatch = pollRecordAndPosBatch();
        // the batch is in flight, return empty to avoid multiple batches to be read
        if (recordAndPosBatch == null) {
            return CloseableIterator.emptyIterator();
        }

        CloseableIterator<ScanRecord> nextBatch = poll();
        if (nextBatch == null) {
            // no any records, add the RecordAndPosBatch back
            recordAndPosBatchPool.add(recordAndPosBatch);
            return null;
        } else {
            return recordAndPosBatch.replace(nextBatch);
        }
    }

    @Nullable
    private RecordAndPosBatch pollRecordAndPosBatch() throws IOException {
        try {
            return this.recordAndPosBatchPool.poll(POLL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted");
        }
    }

    private CloseableIterator<ScanRecord> poll() throws IOException {
        CloseableIterator<ScanRecord> nextBatch = null;
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

    private CloseableIterator<ScanRecord> pollBatch() throws IOException {
        CloseableIterator<ScanRecord> records = splitScanner.poll(POLL_TIMEOUT);
        return records == null ? null : new ScanRecordBatch(records);
    }

    @Override
    public void close() throws Exception {
        splitScanner.close();
    }

    private static class ScanRecordBatch implements CloseableIterator<ScanRecord> {
        private final CloseableIterator<ScanRecord> scanRecordIterator;

        public ScanRecordBatch(CloseableIterator<ScanRecord> scanRecordIterator) {
            this.scanRecordIterator = scanRecordIterator;
        }

        @Override
        public boolean hasNext() {
            return scanRecordIterator.hasNext();
        }

        @Override
        public ScanRecord next() {
            return scanRecordIterator.next();
        }

        @Override
        public void close() {
            scanRecordIterator.close();
        }
    }

    private class RecordAndPosBatch implements CloseableIterator<RecordAndPos> {
        private CloseableIterator<ScanRecord> records;

        private final MutableRecordAndPos recordAndPosition = new MutableRecordAndPos();

        RecordAndPosBatch replace(CloseableIterator<ScanRecord> records) {
            this.records = records;
            recordAndPosition.setRecord(null, NO_READ_RECORDS_COUNT);
            return this;
        }

        @Override
        public boolean hasNext() {
            return records.hasNext();
        }

        @Override
        public RecordAndPos next() {
            recordAndPosition.setRecord(records.next(), ++currentReadRecordsCount);
            return recordAndPosition;
        }

        @Override
        public void close() {
            // close the records
            records.close();
            // add the RecordAndPosBatch back
            recordAndPosBatchPool.add(this);
        }
    }
}
