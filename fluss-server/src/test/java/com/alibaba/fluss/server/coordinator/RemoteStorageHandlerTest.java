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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TablePath;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class RemoteStorageHandlerTest {

    private static @TempDir Path tempDir;

    private static RemoteStorageHandler remoteStorageHandler;

    @BeforeAll
    static void before() throws IOException {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.REMOTE_DATA_DIR, tempDir.toString());
        remoteStorageHandler = new RemoteStorageHandler(conf);
    }

    @Test
    void testDeleteRemoteByTablePath() throws IOException {
        TablePath tablePath = TablePath.of("ods", "tbl");
        remoteStorageHandler.createTableKvDir(tablePath, 0);
        assertThat(remoteStorageHandler.isTableKvDirExists(tablePath, 0)).isTrue();
        remoteStorageHandler.deleteTable(tablePath, 0);
        assertThat(remoteStorageHandler.isTableKvDirExists(tablePath, 0)).isFalse();
    }
}
