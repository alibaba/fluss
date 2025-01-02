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

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.utils.FlussPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RemoteStorageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteStorageHandler.class);

    private final FsPath remoteKvDir;
    private final FileSystem remoteFileSystem;

    public RemoteStorageHandler(Configuration configuration) throws IOException {
        this.remoteKvDir = FlussPaths.remoteKvDir(configuration);
        this.remoteFileSystem = remoteKvDir.getFileSystem();
    }

    public void createTableKvDir(TablePath tablePath, long tableId) throws IOException {
        remoteFileSystem.mkdirs(tableKvDir(tablePath, tableId));
    }

    public void deleteTable(TablePath tablePath, long tableId) {
        deleteDir(tableKvDir(tablePath, tableId));
        // todo delete log segments file dirs of table.
    }

    public boolean isTableKvDirExists(TablePath tablePath, long tableId) throws IOException {
        return isDirExists(tableKvDir(tablePath, tableId));
    }

    private boolean isDirExists(FsPath fsPath) throws IOException {
        return remoteFileSystem.exists(fsPath);
    }

    private void deleteDir(FsPath fsPath) {
        try {
            if (isDirExists(fsPath)) {
                remoteFileSystem.delete(fsPath, true);
                LOG.info("Delete table's remote dir {} success.", fsPath);
            }
        } catch (IOException e) {
            LOG.error("Delete table's remote dir {} failed.", fsPath, e);
        }
    }

    private FsPath tableKvDir(TablePath tablePath, long tableId) {
        return FlussPaths.remoteTableDir(remoteKvDir, tablePath, tableId);
    }
}
