package com.alibaba.fluss.fs.gs;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FileSystemBehaviorTestSuite;
import com.alibaba.fluss.fs.FsPath;

import org.junit.jupiter.api.BeforeAll;

import java.util.UUID;

public class GSFileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {
    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    @BeforeAll
    static void setup() {
        GSTestCredentials.assumeCredentialsAvailable();

        final Configuration conf = new Configuration();
        conf.setString(
                "fs.gs.auth.service.account.json.keyfile",
                GSTestCredentials.getServiceAccountPath());
        conf.setString("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE");
        FileSystem.initialize(conf, null);
    }

    @Override
    protected FileSystem getFileSystem() throws Exception {
        return getBasePath().getFileSystem();
    }

    @Override
    protected FsPath getBasePath() {
        return new FsPath(GSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
    }
}
