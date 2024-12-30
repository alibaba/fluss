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

package com.alibaba.fluss.metadata;

/**
 * Information of a database metadata, includes {@link DatabaseDescriptor}.
 *
 * @since 0.6
 */
public class DatabaseInfo {
    private final String databaseName;
    private final DatabaseDescriptor databaseDescriptor;
    private final long createTime;
    private final long modifyTime;

    public DatabaseInfo(
            String databaseName,
            DatabaseDescriptor databaseDescriptor,
            long createTime,
            long modifyTime) {
        this.databaseName = databaseName;
        this.databaseDescriptor = databaseDescriptor;
        this.createTime = createTime;
        this.modifyTime = modifyTime;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DatabaseDescriptor getDatabaseDescriptor() {
        return databaseDescriptor;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getModifyTime() {
        return modifyTime;
    }
}
