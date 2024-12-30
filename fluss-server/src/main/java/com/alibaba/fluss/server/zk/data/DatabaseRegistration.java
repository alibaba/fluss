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

package com.alibaba.fluss.server.zk.data;

import com.alibaba.fluss.metadata.DatabaseDescriptor;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

/**
 * The registration information of database in {@link ZkData.TableZNode}. It is used to store the
 * database information in zookeeper. Basically, it contains the same information with {@link
 * com.alibaba.fluss.metadata.DatabaseInfo}.
 *
 * @see TableRegistrationJsonSerde for json serialization and deserialization.
 */
public class DatabaseRegistration {
    public final @Nullable String comment;
    public final Map<String, String> properties;
    public final long createTime;
    public final long modifyTime;

    public DatabaseRegistration(
            @Nullable String comment,
            Map<String, String> properties,
            long createTime,
            long modifyTime) {
        this.comment = comment;
        this.properties = properties;
        this.createTime = createTime;
        this.modifyTime = modifyTime;
    }

    public DatabaseDescriptor toDatabaseDescriptor() {
        DatabaseDescriptor.Builder builder = DatabaseDescriptor.builder().comment(comment);
        properties.forEach(builder::property);
        return builder.build();
    }

    public static DatabaseRegistration of(DatabaseDescriptor databaseDescriptor) {
        return new DatabaseRegistration(
                databaseDescriptor.getComment().orElse(null),
                databaseDescriptor.getProperties(),
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatabaseRegistration that = (DatabaseRegistration) o;
        return createTime == that.createTime
                && Objects.equals(comment, that.comment)
                && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(comment, properties, createTime);
    }

    @Override
    public String toString() {
        return "DatabaseRegistration{"
                + "comment='"
                + comment
                + '\''
                + ", properties="
                + properties
                + ", createTime="
                + createTime
                + ", modifyTime="
                + modifyTime
                + '}';
    }
}
