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

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.admin.ClientToServerITCaseBase;
import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA2_ROW_TYPE;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link FlussLogScanner} as scan log from remote. */
public class RemoteLogScannerITCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private Connection conn;
    private Admin admin;

    @BeforeEach
    protected void setup() throws Exception {
        Configuration clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @Test
    void testScanFromRemote() throws Exception {
        TablePath tablePath = TablePath.of("test_db1", "test_scan_from_remote_table_1");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(1).build();
        long tableId = createTable(tablePath, tableDescriptor);

        // append a batch of data.
        int recordSize = 20;
        List<InternalRow> expectedRows = new ArrayList<>();
        Table table = conn.getTable(tablePath);
        AppendWriter appendWriter = table.getAppendWriter();
        for (int i = 0; i < recordSize; i++) {
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, "aaaaa"});
            expectedRows.add(row);
            appendWriter.append(row).get();
        }

        FLUSS_CLUSTER_EXTENSION.waitUtilSomeLogSegmentsCopyToRemote(new TableBucket(tableId, 0));

        // test fetch.
        LogScanner logScanner = table.getLogScanner(new LogScan());
        logScanner.subscribeFromBeginning(0);
        List<InternalRow> rowList = new ArrayList<>();
        while (rowList.size() < recordSize) {
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (ScanRecord scanRecord : scanRecords) {
                assertThat(scanRecord.getRowKind()).isEqualTo(RowKind.APPEND_ONLY);
                InternalRow row = scanRecord.getRow();
                rowList.add(row(DATA1_ROW_TYPE, new Object[] {row.getInt(0), row.getString(1)}));
            }
        }
        assertThat(rowList).hasSize(recordSize);
        assertThat(rowList).containsExactlyInAnyOrderElementsOf(expectedRows);
    }

    @Test
    void testPartitionTableFetchFromRemote() throws Exception {
        final Schema data2NonPkSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .withComment("a is first column")
                        .column("b", DataTypes.STRING())
                        .withComment("b is second column")
                        .column("c", DataTypes.STRING())
                        .withComment("c is adding column")
                        .build();
        final TablePath tablePath =
                new TablePath("test_db1", "test_partition_table_fetch_from_remote");

        TableDescriptor partitionTableDescriptor =
                TableDescriptor.builder()
                        .schema(data2NonPkSchema)
                        .distributedBy(1)
                        .partitionedBy("c")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .build();
        long tableId = createTable(tablePath, partitionTableDescriptor);
        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUtilPartitionAllReady(tablePath);
        Table table = conn.getTable(tablePath);
        AppendWriter appendWriter = table.getAppendWriter();
        int recordsPerPartition = 5;
        Map<Long, List<InternalRow>> expectPartitionAppendRows = new HashMap<>();
        for (String partition : partitionIdByNames.keySet()) {
            for (int i = 0; i < recordsPerPartition; i++) {
                InternalRow row = row(DATA2_ROW_TYPE, new Object[] {i, "aaaab" + i, partition});
                appendWriter.append(row).get();
                expectPartitionAppendRows
                        .computeIfAbsent(partitionIdByNames.get(partition), k -> new ArrayList<>())
                        .add(row);
            }
        }

        for (long id : partitionIdByNames.values()) {
            FLUSS_CLUSTER_EXTENSION.waitUtilSomeLogSegmentsCopyToRemote(
                    new TableBucket(tableId, id, 0));
        }

        ClientToServerITCaseBase.verifyPartitionLogs(
                table, DATA2_ROW_TYPE, expectPartitionAppendRows);
    }

    @AfterEach
    protected void teardown() throws Exception {
        if (admin != null) {
            admin.close();
            admin = null;
        }

        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    private long createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createDatabase(tablePath.getDatabaseName(), false).get();
        admin.createTable(tablePath, tableDescriptor, false).get();
        return admin.getTable(tablePath).get().getTableId();
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a shorter interval for testing purpose
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("1kb"));
        return conf;
    }
}
