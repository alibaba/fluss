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
import com.alibaba.fluss.utils.clock.Clock;
import com.alibaba.fluss.utils.clock.SystemClock;
import com.alibaba.fluss.utils.concurrent.ShutdownableThread;
import com.alibaba.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
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
 * <p>There are five states for the table to be tiered:
 *
 * <ul>
 *   <li>New: when a new lake table is created
 *   <li>ReInitialized: when the coordinator server is restarted, the state of all existing lake
 *       table will be ReInitialized state
 *   <li>Scheduled: when the lake table is waiting for a period of time to be tiered
 *   <li>Pending: when the lake table is waiting for tiering service to request the table
 *   <li>Tiering: when the lake table is being tiered by tiering service
 * </ul>
 *
 * <p>The state machine of table to be tiered is as follows:
 *
 * <pre>{@code
 * New -> Scheduled
 * ReInitialized --> Scheduled
 * Scheduled --> |after waiting for a period of tiering interval| Pending
 * Pending --> |lake tiering service request table| Tiering
 * Tiering --> |once lake tiering finish| Scheduled
 * Tiering --> |lake tiering heartbeat timout| Pending
 * }</pre>
 */
public class LakeTableTieringManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LakeTableTieringManager.class);

    protected static final long TIERING_SERVICE_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes

    private final Timer lakeTieringScheduleTimer;
    private final Clock clock;
    private final Queue<Long> pendingTieringTables;
    private final LakeTieringExpiredOperationReaper expirationReaper;

    // the tiering state of the table to be tiered,
    // from table_id -> tiering state
    private final Map<Long, TieringState> tieringStates;

    // the live tables that are tiering,
    // from table_id -> last heartbeat time by the tiering service
    private final Map<Long, Long> liveTieringTableIds;

    // table_id -> table path
    private final Map<Long, TablePath> tablePathById;

    // table_id -> tiering interval
    private final Map<Long, Long> tieringIntervalByTableId;

    private final Lock lock = new ReentrantLock();

    public LakeTableTieringManager() {
        this(new DefaultTimer("delay lake tiering", 1_000, 20), SystemClock.getInstance());
    }

    @VisibleForTesting
    protected LakeTableTieringManager(Timer lakeTieringScheduleTimer, Clock clock) {
        this.lakeTieringScheduleTimer = lakeTieringScheduleTimer;
        this.clock = clock;
        this.pendingTieringTables = new ArrayDeque<>();
        this.tieringStates = new HashMap<>();
        this.liveTieringTableIds = new HashMap<>();
        this.tablePathById = new HashMap<>();
        this.tieringIntervalByTableId = new HashMap<>();
        this.expirationReaper = new LakeTieringExpiredOperationReaper();
        expirationReaper.start();
    }

    public void initWithLakeTables(List<TableInfo> lakeTables) {
        for (TableInfo tableInfo : lakeTables) {
            doHandleStateChange(tableInfo.getTableId(), TieringState.ReInitialized);
            addLakeTableToTier(tableInfo);
        }
    }

    public void addNewLakeTable(TableInfo tableInfo) {
        doHandleStateChange(tableInfo.getTableId(), TieringState.New);
        addLakeTableToTier(tableInfo);
    }

    private void addLakeTableToTier(TableInfo tableInfo) {
        inLock(
                lock,
                () -> {
                    tablePathById.put(tableInfo.getTableId(), tableInfo.getTablePath());
                    tieringIntervalByTableId.put(
                            tableInfo.getTableId(),
                            tableInfo.getTableConfig().getDataLakeTieringInterval().toMillis());
                    doHandleStateChange(tableInfo.getTableId(), TieringState.Scheduled);
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
                    tieringStates.remove(tableId);
                });
    }

    @Nullable
    public Tuple2<Long, TablePath> requestTable() {
        return inLock(
                lock,
                () -> {
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
                    registerTieringTimeout(tableId);
                    doHandleStateChange(tableId, TieringState.Tiering);
                    return new Tuple2<>(tableId, tablePath);
                });
    }

    private void registerTieringTimeout(long tableId) {
        // we put the table to a timout timer,
        // if no liveness heartbeat for the table in this period of time, we consider the tiering is
        // invalid and put it back to Pending state to wait to be requested by other tiering service
        lakeTieringScheduleTimer.add(
                new TimerTask(TIERING_SERVICE_TIMEOUT_MS) {
                    @Override
                    public void run() {
                        inLock(
                                lock,
                                () -> {
                                    if (isTieringServiceTimeout(tableId)) {
                                        // change it to pending to wait to be requested by other
                                        // lake tiering service
                                        doHandleStateChange(tableId, TieringState.Pending);
                                    } else {
                                        // register the next round of tiering
                                        // timeout again
                                        registerTieringTimeout(tableId);
                                    }
                                });
                    }
                });
    }

    private boolean isTieringServiceTimeout(long tableId) {
        Long lastHeartbeat = liveTieringTableIds.get(tableId);
        return lastHeartbeat == null
                || clock.milliseconds() - lastHeartbeat >= TIERING_SERVICE_TIMEOUT_MS;
    }

    public void finishTableTiering(long tableId) {
        inLock(
                lock,
                () ->
                        // schedule it again if one round of tiering is finished
                        doHandleStateChange(tableId, TieringState.Scheduled));
    }

    public void renewTieringHeartbeat(long tableId) {
        inLock(
                lock,
                () -> {
                    liveTieringTableIds.put(tableId, clock.milliseconds());
                });
    }

    /**
     * Handle the state change of the lake table to be tiered. The core state transitions for the
     * state machine are as follows:
     *
     * <p>New -> Scheduled:
     *
     * <p>-- When the lake table is newly created, do: schedule a timer to wait for a tiering
     * interval configured in table, transmit the table to Pending
     *
     * <p>ReInitialized -> Scheduled：
     *
     * <p>-- When the coordinator server is restarted, for all existing lake table, do: schedule a
     * timer to wait for a tiering service timeout, transmit the tables to Pending. Note, we must
     * wait for the tiering service timeout in case any existing tiering service is tiering the
     * table
     *
     * <p>Tiering -> Scheduled
     *
     * <p>-- When the tiering service finished one-round of tiering, do: schedule a timer to wait
     * for a tiering interval configured in table, transmit the table to Pending
     *
     * <p>Scheduled -> Pending
     *
     * <p>-- The time interval to wait has passed, do: transmit to Pending state
     *
     * <p>Tiering -> Pending
     *
     * <p>-- When the tiering service is timeout, do: transmit to Pending state
     *
     * <p>Pending -> Tiering
     *
     * <p>-- When the table is assigned to a tiering service after tiering service request the
     * table, do: transmit to Tiering state
     */
    private void doHandleStateChange(long tableId, TieringState targetState) {
        TieringState currentState = tieringStates.get(tableId);
        if (!isValidStateTransition(currentState, targetState)) {
            LOG.error(
                    "Fail to change state for table {} from {} to {} as it's not a valid state change.",
                    tableId,
                    currentState,
                    targetState);
            return;
        }
        switch (targetState) {
            case New:
            case ReInitialized:
                // do nothing
                break;
            case Scheduled:
                if (currentState == TieringState.New) {
                    scheduleTableTiering(tableId);
                } else if (currentState == TieringState.ReInitialized) {
                    // schedule the table, but we wait for a period of TIMEOUT
                    // to avoid the existing tiering services are still tiering it since
                    // it's a table reinitialized by coordinator server
                    scheduleTableTiering(tableId, TIERING_SERVICE_TIMEOUT_MS);
                } else if (currentState == TieringState.Tiering) {
                    // should happens the table has finished one-round of tiering,
                    // schedule it again
                    liveTieringTableIds.remove(tableId);
                    scheduleTableTiering(tableId);
                }
                break;
            case Pending:
                pendingTieringTables.add(tableId);
                break;
            case Tiering:
                // do nothing
                break;
        }
        doStateChange(tableId, currentState, targetState);
    }

    private boolean isValidStateTransition(
            @Nullable TieringState curState, TieringState targetState) {
        if (targetState == TieringState.New || targetState == TieringState.ReInitialized) {
            // when target state is new or reinitialized, it's valid when current state is null
            return curState == null;
        }
        return targetState.validPreviousStates().contains(curState);
    }

    private void doStateChange(long tableId, TieringState fromState, TieringState toState) {
        tieringStates.put(tableId, toState);
        LOG.debug(
                "Successfully changed state for table {} from {} to {}.",
                tableId,
                fromState,
                fromState);
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
            throw new FlussRuntimeException(
                    "Error while shutdown lake tiering expired operation manager", e);
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
                    () ->
                            // to pending state
                            doHandleStateChange(tableId, TieringState.Pending));
        }
    }

    private class LakeTieringExpiredOperationReaper extends ShutdownableThread {

        public LakeTieringExpiredOperationReaper() {
            super("LakeTieringExpiredOperationReaper", false);
        }

        @Override
        public void doWork() throws Exception {
            advanceClock();
        }

        private void advanceClock() throws InterruptedException {
            lakeTieringScheduleTimer.advanceClock(200L);
        }
    }

    private enum TieringState {
        // When a new lake table is created, the state will be New
        New {
            @Override
            public Set<TieringState> validPreviousStates() {
                return Collections.emptySet();
            }
        },
        // When the coordinator server is restarted, the state of existing lake table
        // will be ReInitialized
        ReInitialized {
            @Override
            public Set<TieringState> validPreviousStates() {
                return Collections.emptySet();
            }
        },
        // When the lake table is waiting to be tiered, such as waiting for the period of tiering
        // interval, the state will be Scheduled
        Scheduled {
            @Override
            public Set<TieringState> validPreviousStates() {
                return EnumSet.of(New, ReInitialized, Tiering);
            }
        },
        // When the period of tiering interval has passed, but no any tiering service requesting
        // table, the state will be Pending
        Pending {
            @Override
            public Set<TieringState> validPreviousStates() {
                return EnumSet.of(Scheduled, Tiering);
            }
        },
        // When one tiering service is tiering the table, the state will be Tiering
        Tiering {
            @Override
            public Set<TieringState> validPreviousStates() {
                return EnumSet.of(Pending);
            }
        };

        abstract Set<TieringState> validPreviousStates();
    }
}
