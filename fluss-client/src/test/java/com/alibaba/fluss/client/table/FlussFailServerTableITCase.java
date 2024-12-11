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

package com.alibaba.fluss.client.table;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.client.scanner.log.LogScan;
import com.alibaba.fluss.client.scanner.log.LogScanner;
import com.alibaba.fluss.client.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.client.admin.ClientToServerITCaseBase.initConfig;
import static com.alibaba.fluss.client.admin.ClientToServerITCaseBase.subscribeFromBeginning;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static com.alibaba.fluss.testutils.InternalRowListAssert.assertThatRows;

/** IT case for {@link FlussTable} in the case of one tablet server fails. */
class FlussFailServerTableITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private static final int SERVER = 0;
    private Connection conn;
    protected Admin admin;

    @BeforeEach
    void beforeEach() {
        Configuration clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();

        // since we kill and start one tablet server in each test,
        // we need to wait for metadata to be updated to servers
        FLUSS_CLUSTER_EXTENSION.waitUtilAllGatewayHasSameMetadata();
    }

    @Test
    void testAppend() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_fail_append_table_1");
        admin.createDatabase(tablePath.getDatabaseName(), true).get();
        admin.createTable(tablePath, DATA1_TABLE_INFO.getTableDescriptor(), false).get();
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter appendWriter = table.getAppendWriter();
            IndexedRow row = row(DATA1_ROW_TYPE, new Object[] {1, "a"});

            // append a row
            appendWriter.append(row).get();

            // now, kill one server and try to append data again
            FLUSS_CLUSTER_EXTENSION.stopTabletServer(SERVER);

            try {
                // append some rows again, should success
                for (int i = 0; i < 10; i++) {
                    appendWriter.append(row).get();
                }
            } finally {
                FLUSS_CLUSTER_EXTENSION.startTabletServer(SERVER);
            }
        }
    }

    @Test
    void testPut() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_fail_kv_table_1");
        admin.createDatabase(tablePath.getDatabaseName(), true).get();
        admin.createTable(tablePath, DATA1_TABLE_INFO_PK.getTableDescriptor(), false).get();
        // put one row
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.getUpsertWriter();
            InternalRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
            upsertWriter.upsert(row).get();

            // kill first tablet server
            FLUSS_CLUSTER_EXTENSION.stopTabletServer(SERVER);

            try {
                // append some rows again, should success
                for (int i = 0; i < 10; i++) {
                    // mock a row
                    row = compactedRow(DATA1_ROW_TYPE, new Object[] {i, "a" + i});
                    upsertWriter.upsert(row).get();
                }
            } finally {
                // todo: try to get value when get is re-triable in FLUSS-56857409
                FLUSS_CLUSTER_EXTENSION.startTabletServer(SERVER);
            }
        }
    }

    @Test
    void testLogScan() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_fail_log_scan_table_1");
        admin.createDatabase(tablePath.getDatabaseName(), true).get();
        admin.createTable(tablePath, DATA1_TABLE_INFO.getTableDescriptor(), false).get();
        // append one row.
        IndexedRow row = row(DATA1_ROW_TYPE, new Object[] {1, "a"});
        try (Table table = conn.getTable(tablePath);
                LogScanner logScanner = table.getLogScanner(new LogScan())) {
            subscribeFromBeginning(logScanner, table);
            AppendWriter appendWriter = table.getAppendWriter();
            appendWriter.append(row).get();

            // poll data util we get one record
            ScanRecords scanRecords;
            do {
                scanRecords = logScanner.poll(Duration.ofSeconds(1));
            } while (scanRecords.isEmpty());

            int rowCount = 10;
            // append some rows
            List<IndexedRow> expectRows = new ArrayList<>(rowCount);
            for (int i = 0; i < rowCount; i++) {
                appendWriter.append(row).get();
                expectRows.add(row);
            }
            // kill first tablet server
            FLUSS_CLUSTER_EXTENSION.stopTabletServer(SERVER);

            try {
                // now, poll records utils we can get all the records
                int counts = 0;
                int expectCounts = 10;
                List<InternalRow> actualRows = new ArrayList<>(rowCount);
                do {
                    scanRecords = logScanner.poll(Duration.ofSeconds(1));
                    actualRows.addAll(toRows(scanRecords));
                    counts += scanRecords.count();
                } while (counts < expectCounts);
                assertThatRows(actualRows).withSchema(DATA1_ROW_TYPE).isEqualTo(expectRows);
            } finally {
                FLUSS_CLUSTER_EXTENSION.startTabletServer(SERVER);
            }
        }
    }

    private List<InternalRow> toRows(ScanRecords scanRecords) {
        List<InternalRow> rows = new ArrayList<>();
        for (ScanRecord scanRecord : scanRecords) {
            rows.add(scanRecord.getRow());
        }
        return rows;
    }
}
