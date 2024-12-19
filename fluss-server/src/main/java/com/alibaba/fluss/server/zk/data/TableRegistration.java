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

import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The registration information of table in {@link ZkData.TableZNode}. It is used to store the table
 * information in zookeeper. Basically, it contains the same information with {@link TableInfo}
 * besides the {@link Schema} part and schema id. Because schema metadata is stored in a separate
 * {@code SchemaZNode}.
 *
 * @see TableRegistrationJsonSerde for json serialization and deserialization.
 */
public class TableRegistration {

    public final long tableId;
    public final @Nullable String comment;
    public final List<String> partitionKeys;
    public final @Nullable TableDescriptor.TableDistribution tableDistribution;
    public final Map<String, String> properties;
    public final Map<String, String> customProperties;

    public TableRegistration(
            long tableId,
            @Nullable String comment,
            List<String> partitionKeys,
            @Nullable TableDescriptor.TableDistribution tableDistribution,
            Map<String, String> properties,
            Map<String, String> customProperties) {
        this.tableId = tableId;
        this.comment = comment;
        this.partitionKeys = partitionKeys;
        this.tableDistribution = tableDistribution;
        this.properties = properties;
        this.customProperties = customProperties;
    }

    public TableDescriptor toTableDescriptor(Schema schema) {
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(schema)
                        .comment(comment)
                        .partitionedBy(partitionKeys);
        if (tableDistribution != null) {
            builder.distributedBy(
                    tableDistribution.getBucketCount().orElse(null),
                    tableDistribution.getBucketKeys());
        }
        properties.forEach(builder::property);
        customProperties.forEach(builder::customProperty);
        return builder.build();
    }

    public static TableRegistration of(long tableId, TableDescriptor tableDescriptor) {
        return new TableRegistration(
                tableId,
                tableDescriptor.getComment().orElse(null),
                tableDescriptor.getPartitionKeys(),
                tableDescriptor.getTableDistribution().orElse(null),
                tableDescriptor.getProperties(),
                tableDescriptor.getCustomProperties());
    }

    public TableRegistration copy(
            Map<String, String> newProperties, Map<String, String> newCustomProperties) {
        return new TableRegistration(
                tableId,
                comment,
                partitionKeys,
                tableDistribution,
                newProperties,
                newCustomProperties);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableRegistration that = (TableRegistration) o;
        return tableId == that.tableId
                && Objects.equals(comment, that.comment)
                && Objects.equals(partitionKeys, that.partitionKeys)
                && Objects.equals(tableDistribution, that.tableDistribution)
                && Objects.equals(properties, that.properties)
                && Objects.equals(customProperties, that.customProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tableId, comment, partitionKeys, tableDistribution, properties, customProperties);
    }

    @Override
    public String toString() {
        return "TableRegistration{"
                + "tableId="
                + tableId
                + ", comment='"
                + comment
                + '\''
                + ", partitionKeys="
                + partitionKeys
                + ", tableDistribution="
                + tableDistribution
                + ", properties="
                + properties
                + ", customProperties="
                + customProperties
                + '}';
    }
}
