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

package com.alibaba.fluss.fs.gs;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FSDataInputStream;
import com.alibaba.fluss.fs.FSDataOutputStream;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

class GSFileSystemPluginTest {

    private MockGSServer mockGSServer;

    @BeforeEach
    void setUp() {
        mockGSServer = MockGSServer.create("test-bucket", "fluss", "test");
    }

    @Test
    void testWithPluginManager() throws Exception {
        FileSystem fileSystem = createFileSystem();

        String basePath = "gs://test-bucket/fluss";
        assertThat(FileSystem.get(URI.create(basePath))).isInstanceOf(GSFileSystem.class);

        FsPath path = new FsPath(basePath, "test");
        final FSDataOutputStream outputStream =
                fileSystem.create(path, FileSystem.WriteMode.OVERWRITE);
        final byte[] testbytes = {1, 2, 3, 4, 5};
        outputStream.write(testbytes);
        outputStream.close();

        // check the path
        assertThat(fileSystem.exists(path)).isTrue();

        // try to read the file
        byte[] testbytesRead = new byte[5];
        FSDataInputStream inputStream = fileSystem.open(path);
        assertThat(5).isEqualTo(inputStream.read(testbytesRead));
        inputStream.close();
        assertThat(testbytesRead).isEqualTo(testbytes);

        assertThat(fileSystem.exists(path)).isTrue();

        // try to seek the file
        inputStream = fileSystem.open(path);
        inputStream.seek(4);
        testbytesRead = new byte[1];
        assertThat(1).isEqualTo(inputStream.read(testbytesRead));
        assertThat(testbytesRead).isEqualTo(new byte[] {testbytes[4]});

        // now delete the file
        assertThat(fileSystem.delete(path, true)).isTrue();
        // get the status of the file should throw exception
        assertThatThrownBy(() -> fileSystem.getFileStatus(path))
                .isInstanceOf(FileNotFoundException.class);
    }

    private static FileSystem createFileSystem() throws IOException {
        String path =
                GSFileSystemPlugin.class
                        .getClassLoader()
                        .getResource("fake-service-account.json")
                        .getPath();

        GSFileSystemPlugin gsFileSystemPlugin = new GSFileSystemPlugin();
        Configuration configuration = new Configuration();
        configuration.setString("fs.gs.storage.root.url", "http://localhost:8080");
        configuration.setString("fs.gs.token.server.url", "http://localhost:8080/token");
        configuration.setString("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE");
        configuration.setString("fs.gs.auth.service.account.json.keyfile", path);
        configuration.setString("fs.gs.inputstream.support.gzip.encoding.enable", "false");
        return gsFileSystemPlugin.create(URI.create("gs://test-bucket/flusspath"), configuration);
    }

    @AfterEach
    void tearDown() throws IOException {
        mockGSServer.close();
    }
}
