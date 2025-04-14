/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.utils.timer.DefaultTimer;
import com.alibaba.fluss.server.utils.timer.Timer;
import com.alibaba.fluss.server.utils.timer.TimerTask;
import com.alibaba.fluss.utils.concurrent.ShutdownableThread;
import com.alibaba.fluss.utils.types.Tuple2;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/**
 * A manager to manage the tables to be tiered.
 *
 * <p>For a lake table to be tiered, when created, it wil be put into this manager and scheduled to
 * be tiered by tiering services.
 *
 * <p>The state machine of table to be tiered is as follows:
 *
 * <pre>{@code
 * New -> Scheduled
 * Scheduled --> |after lake tiering interval| Pending
 * Pending --> |lake tiering service request table| Tiering
 * Tiering --> |once lake tiering finish| Scheduled
 * Tiering --> |lake tiering heartbeat timout| Scheduled
 * }</pre>
 */
public class LakeTableTieringManager implements AutoCloseable {

    protected static final long TIERING_SERVICE_TIMEOUT_MS = 3 * 60 * 1000; // 3 minutes

    private final Timer lakeTieringScheduleTimer;
    private final Queue<Long> pendingTieringTables;
    private final ExpiredOperationReaper expirationReaper;

    // the live tables that are tiering
    private final Set<Long> liveTieringTableIds;

    // table_id -> table path
    private final Map<Long, TablePath> tablePathById;

    // table_id -> tiering interval
    private final Map<Long, Long> tieringIntervalByTableId;

    private final Lock lock = new ReentrantLock();

    public LakeTableTieringManager() {
        this(new DefaultTimer("delay lake tiering", 1_000, 20));
    }

    @VisibleForTesting
    protected LakeTableTieringManager(Timer lakeTieringScheduleTimer) {
        this.lakeTieringScheduleTimer = lakeTieringScheduleTimer;
        this.pendingTieringTables = new ArrayDeque<>();
        this.liveTieringTableIds = new HashSet<>();
        this.tablePathById = new HashMap<>();
        this.tieringIntervalByTableId = new HashMap<>();
        this.expirationReaper = new ExpiredOperationReaper();
        expirationReaper.start();
    }

    public void initWithLakeTables(List<TableInfo> lakeTables) {
        for (TableInfo tableInfo : lakeTables) {
            // try to schedule the tables to be tiered, we wait for a period of TIMEOUT
            // to avoid the existing tiering services are still tiering it
            addLakeTable(tableInfo, TIERING_SERVICE_TIMEOUT_MS);
        }
    }

    public void addLakeTable(TableInfo tableInfo) {
        addLakeTable(tableInfo, null);
    }

    private void addLakeTable(TableInfo tableInfo, @Nullable Long delayMs) {
        inLock(
                lock,
                () -> {
                    tablePathById.put(tableInfo.getTableId(), tableInfo.getTablePath());
                    tieringIntervalByTableId.put(
                            tableInfo.getTableId(),
                            tableInfo.getTableConfig().getDataLakeTieringInterval().toMillis());
                    scheduleTableTiering(tableInfo.getTableId(), delayMs);
                });
    }

    private void scheduleTableTiering(long tableId) {
        scheduleTableTiering(tableId, null);
    }

    private void scheduleTableTiering(long tableId, @Nullable Long delayMs) {
        Long tieringInterval = tieringIntervalByTableId.get(tableId);
        if (tieringInterval == null) {
            // the table has been dropped, return directly
            return;
        }
        lakeTieringScheduleTimer.add(
                new DelayedTiering(tableId, delayMs == null ? tieringInterval : delayMs));
    }

    public void removeLakeTable(long tableId) {
        inLock(
                lock,
                () -> {
                    tablePathById.remove(tableId);
                    tieringIntervalByTableId.remove(tableId);
                });
    }

    @Nullable
    public Tuple2<Long, TablePath> requestTable() {
        Long tableId = pendingTieringTables.poll();
        // no any pending table, return directly
        if (tableId == null) {
            return null;
        }
        TablePath tablePath = tablePathById.get(tableId);
        // the table has been dropped, request again
        if (tablePath == null) {
            return requestTable();
        }
        tieringLivenessProbe(tableId);
        return new Tuple2<>(tableId, tablePath);
    }

    private void tieringLivenessProbe(long tableId) {
        // we put the table to a timout timer,
        // if no liveness for the table in this period of time, we consider the tiering is
        // invalid and put it back to wait to be scheduled by other tiering service
        lakeTieringScheduleTimer.add(
                new TimerTask(TIERING_SERVICE_TIMEOUT_MS) {
                    @Override
                    public void run() {
                        inLock(
                                lock,
                                () -> {
                                    // the table is not in tiering, we put it back to wait to be
                                    // scheduled
                                    if (!liveTieringTableIds.remove(tableId)) {
                                        scheduleTableTiering(tableId);
                                    } else {
                                        // schedule liveness probe again
                                        tieringLivenessProbe(tableId);
                                    }
                                });
                    }
                });
    }

    public void finishTableTiering(long tableId) {
        inLock(
                lock,
                () -> {
                    liveTieringTableIds.remove(tableId);
                    scheduleTableTiering(tableId);
                });
    }

    public void renewTieringLiveness(long tableId) {
        inLock(
                lock,
                () -> {
                    liveTieringTableIds.add(tableId);
                });
    }

    @Override
    public void close() throws Exception {
        expirationReaper.initiateShutdown();
        // improve shutdown time by waking up any ShutdownableThread(s) blocked on poll by
        // sending a no-op.
        lakeTieringScheduleTimer.add(
                new TimerTask(0) {
                    @Override
                    public void run() {}
                });
        try {
            expirationReaper.awaitShutdown();
        } catch (InterruptedException e) {
            throw new FlussRuntimeException("Error while shutdown delayed operation manager", e);
        }

        lakeTieringScheduleTimer.shutdown();
    }

    private class DelayedTiering extends TimerTask {

        private final long tableId;

        public DelayedTiering(long tableId, long delayMs) {
            super(delayMs);
            this.tableId = tableId;
        }

        @Override
        public void run() {
            inLock(
                    lock,
                    () -> {
                        if (!liveTieringTableIds.contains(tableId)) {
                            pendingTieringTables.add(tableId);
                        }
                    });
        }
    }

    private class ExpiredOperationReaper extends ShutdownableThread {

        public ExpiredOperationReaper() {
            super("ExpiredOperationReaper", false);
        }

        @Override
        public void doWork() throws Exception {
            advanceClock();
        }

        private void advanceClock() throws InterruptedException {
            lakeTieringScheduleTimer.advanceClock(200L);
        }
    }
}
