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

import com.alibaba.fluss.annotation.PublicEvolving;

import java.util.Objects;

/**
 * Information of a table metadata, includes {@link TableDescriptor} and the table id that
 * represents the unique identifier of the table. This makes sure that tables recreated with the
 * same name will always have unique table identifiers. It also contains a schema id which
 * identifies the schema of the table like {@link SchemaInfo} does.
 *
 * @since 0.1
 */
@PublicEvolving
public final class TableInfo {

    public static final long UNKNOWN_TABLE_ID = -1L;

    private final TablePath tablePath;
    private final TableDescriptor tableDescriptor;
    private final long tableId;
    private final int schemaId;
    private final long createTime;
    private final long modifyTime;

    public TableInfo(
            TablePath tablePath, long tableId, TableDescriptor tableDescriptor, int schemaId) {
        this(tablePath, tableId, tableDescriptor, schemaId, -1, -1);
    }

    public TableInfo(
            TablePath tablePath,
            long tableId,
            TableDescriptor tableDescriptor,
            int schemaId,
            long createTime,
            long modifyTime) {
        this.tablePath = tablePath;
        this.tableId = tableId;
        this.tableDescriptor = tableDescriptor;
        this.schemaId = schemaId;
        this.createTime = createTime;
        this.modifyTime = modifyTime;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public TableDescriptor getTableDescriptor() {
        return tableDescriptor;
    }

    public long getTableId() {
        return tableId;
    }

    public int getSchemaId() {
        return schemaId;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getModifyTime() {
        return modifyTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableInfo tableInfo = (TableInfo) o;
        return tableId == tableInfo.tableId
                && schemaId == tableInfo.schemaId
                && Objects.equals(tablePath, tableInfo.tablePath)
                && Objects.equals(tableDescriptor, tableInfo.tableDescriptor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePath, tableDescriptor, tableId, schemaId, createTime);
    }

    @Override
    public String toString() {
        return "TableInfo{"
                + "tablePath="
                + tablePath
                + ", tableId="
                + tableId
                + ", schemaId="
                + schemaId
                + ", createTime="
                + createTime
                + ", tableDescriptor="
                + tableDescriptor
                + '}';
    }
}
