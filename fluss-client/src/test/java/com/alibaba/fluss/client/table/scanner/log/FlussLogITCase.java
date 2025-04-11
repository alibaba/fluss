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

package com.alibaba.fluss.client.table.scanner.log;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for subscribing ttl log. */
public class FlussLogITCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("1kb"));
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        return conf;
    }

    protected Connection conn;
    protected Admin admin;
    protected Configuration clientConf;

    @BeforeEach
    protected void setup() throws Exception {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @Test()
    void testSubscribeTTLLog() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "testSubscribeTTLLog");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .build())
                        // ttl eagerly
                        .property(ConfigOptions.TABLE_LOG_TTL, Duration.ofSeconds(1))
                        .distributedBy(1)
                        .build();
        createTable(tablePath, tableDescriptor, false);
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (int n = 0; n < 10; n++) {
                for (int i = 0; i < 10; i++) {
                    appendWriter.append(row(1, "a"));
                }
                appendWriter.flush();
            }

            // wait 5s for log is ttl
            Thread.sleep(5_000);

            try (LogScanner logScanner = table.newScan().createLogScanner()) {
                logScanner.subscribe(0, 2);

                assertThatThrownBy(
                                () -> {
                                    ScanRecords scanRecords =
                                            logScanner.poll(Duration.ofSeconds(1));
                                    System.out.println(scanRecords);
                                })
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("LogOffsetOutOfRangeException");
            }
        }
    }

    protected long createTable(
            TablePath tablePath, TableDescriptor tableDescriptor, boolean ignoreIfExists)
            throws Exception {
        admin.createDatabase(tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, ignoreIfExists)
                .get();
        admin.createTable(tablePath, tableDescriptor, ignoreIfExists).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }
}
