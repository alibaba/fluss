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
