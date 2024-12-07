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

import com.alibaba.fluss.config.FlussConfigUtils;
import com.alibaba.fluss.exception.DatabaseAlreadyExistException;
import com.alibaba.fluss.exception.DatabaseNotEmptyException;
import com.alibaba.fluss.exception.DatabaseNotExistException;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.InvalidAlterTableException;
import com.alibaba.fluss.exception.SchemaNotExistException;
import com.alibaba.fluss.exception.TableAlreadyExistException;
import com.alibaba.fluss.exception.TableNotExistException;
import com.alibaba.fluss.exception.TableNotPartitionedException;
import com.alibaba.fluss.metadata.SchemaInfo;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metadata.UpdateProperties;
import com.alibaba.fluss.server.entity.AlterTableData;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.server.zk.data.TableRegistration;
import com.alibaba.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import com.alibaba.fluss.utils.function.RunnableWithException;
import com.alibaba.fluss.utils.function.ThrowingRunnable;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

import static com.alibaba.fluss.config.FlussConfigUtils.TABLE_OPTIONS_SUPPORT_ALTER;

/** A manager for metadata. */
public class MetaDataManager {
    private final ZooKeeperClient zookeeperClient;

    public MetaDataManager(ZooKeeperClient zookeeperClient) {
        this.zookeeperClient = zookeeperClient;
    }

    public void createDatabase(String databaseName, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        if (databaseExists(databaseName)) {
            if (ignoreIfExists) {
                return;
            }
            throw new DatabaseAlreadyExistException(
                    "Database " + databaseName + " already exists.");
        }

        uncheck(
                () -> zookeeperClient.registerDatabase(databaseName),
                "Fail to create database: " + databaseName);
    }

    public boolean databaseExists(String databaseName) {
        return uncheck(
                () -> zookeeperClient.databaseExists(databaseName),
                "Fail to check database exists or not");
    }

    public List<String> listDatabases() {
        return uncheck(zookeeperClient::listDatabases, "Fail to list database");
    }

    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException("Database " + databaseName + " does not exist.");
        }
        return uncheck(
                () -> zookeeperClient.listTables(databaseName),
                "Fail to list tables for database:" + databaseName);
    }

    /**
     * List the partitions of the given table.
     *
     * <p>Return a map from partition name to partition id.
     */
    public Map<String, Long> listPartitions(TablePath tablePath)
            throws TableNotExistException, TableNotPartitionedException {
        TableInfo tableInfo = getTable(tablePath);
        if (!tableInfo.getTableDescriptor().isPartitioned()) {
            throw new TableNotPartitionedException(
                    "Table '" + tablePath + "' is not a partitioned table.");
        }
        return uncheck(
                () -> zookeeperClient.getPartitionNameAndIds(tablePath),
                "Fail to list partitions for table: " + tablePath);
    }

    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        if (!databaseExists(name)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new DatabaseNotExistException("Database " + name + " does not exist.");
        }
        if (!cascade && !listTables(name).isEmpty()) {
            throw new DatabaseNotEmptyException("Database " + name + " is not empty.");
        }

        uncheck(() -> zookeeperClient.deleteDatabase(name), "Fail to drop database: " + name);
    }

    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException("Table " + tablePath + " does not exist.");
        }

        // in here, we just delete the table node in zookeeper, which will then trigger
        // the physical deletion in tablet servers and assignments in zk
        uncheck(() -> zookeeperClient.deleteTable(tablePath), "Fail to drop table: " + tablePath);
    }

    public void alterTable(AlterTableData alterTableData) {
        TablePath tablePath = alterTableData.getTablePath();
        if (!tableExists(tablePath)) {
            throw new TableNotExistException("Table " + tablePath + " does not exist.");
        }

        // validate and apply update properties.
        TableRegistration newTableReg =
                validateAndApplyUpdateProperties(
                        tablePath, alterTableData.getUpdatePropertiesData());

        uncheck(
                () -> zookeeperClient.updateTable(tablePath, newTableReg),
                "Fail to alter table: " + tablePath);
    }

    public void completeDeleteTable(long tableId) {
        // final step for delete a table.
        // delete bucket assignments node, which will also delete the bucket state node,
        // so that all the zk nodes related to this table are deleted.
        rethrowIfIsNotNoNodeException(
                () -> zookeeperClient.deleteTableAssignment(tableId),
                String.format("Delete tablet assignment meta fail for table %s.", tableId));
    }

    public void completeDeletePartition(long partitionId) {
        // final step for delete a partition.
        // delete partition assignments node, which will also delete the bucket state node,
        // so that all the zk nodes related to this partition are deleted.
        rethrowIfIsNotNoNodeException(
                () -> zookeeperClient.deletePartitionAssignment(partitionId),
                String.format("Delete tablet assignment meta fail for partition %s.", partitionId));
    }

    /**
     * Creates the necessary metadata of the given table in zookeeper and return the table id.
     * Returns -1 if the table already exists and ignoreIfExists is true.
     *
     * @param tablePath the table path
     * @param tableDescriptor the table descriptor
     * @param tableAssignment the table assignment, will be null when the table is partitioned table
     * @param ignoreIfExists whether to ignore if the table already exists
     * @return the table id
     */
    public long createTable(
            TablePath tablePath,
            TableDescriptor tableDescriptor,
            @Nullable TableAssignment tableAssignment,
            boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(
                    "Database " + tablePath.getDatabaseName() + " does not exist.");
        }
        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                return -1;
            } else {
                throw new TableAlreadyExistException("Table " + tablePath + " already exists.");
            }
        }

        // validate all table properties are known to Fluss and property values are valid.
        FlussConfigUtils.validateTableProperties(tableDescriptor.getProperties());

        // register schema to zk
        // first register a schema to the zk, if then register the table
        // to zk fails, there's no harm to register a new schema to zk again
        try {
            zookeeperClient.registerSchema(tablePath, tableDescriptor.getSchema());
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    "Fail to register schema when creating table " + tablePath, e);
        }

        // register the table, we have registered the schema whose path have contained the node for
        // the table, then we won't need to create the node to store the table
        return uncheck(
                () -> {
                    // generate a table id
                    long tableId = zookeeperClient.getTableIdAndIncrement();
                    if (tableAssignment != null) {
                        // register table assignment
                        zookeeperClient.registerTableAssignment(tableId, tableAssignment);
                    }
                    // register the table
                    zookeeperClient.registerTable(
                            tablePath, TableRegistration.of(tableId, tableDescriptor), false);
                    return tableId;
                },
                "Fail to create table " + tablePath);
    }

    public TableInfo getTable(TablePath tablePath) throws TableNotExistException {
        TableRegistration tableReg = getTableRegistration(tablePath);
        SchemaInfo schemaInfo = getLatestSchema(tablePath);
        return new TableInfo(
                tablePath,
                tableReg.tableId,
                tableReg.toTableDescriptor(schemaInfo.getSchema()),
                schemaInfo.getSchemaId());
    }

    private TableRegistration getTableRegistration(TablePath tablePath)
            throws TableNotExistException {
        Optional<TableRegistration> optionalTable;
        try {
            optionalTable = zookeeperClient.getTable(tablePath);
        } catch (Exception e) {
            throw new FlussRuntimeException(String.format("Fail to get table '%s'.", tablePath), e);
        }
        if (!optionalTable.isPresent()) {
            throw new TableNotExistException("Table '" + tablePath + "' does not exist.");
        }
        return optionalTable.get();
    }

    public SchemaInfo getLatestSchema(TablePath tablePath) throws SchemaNotExistException {
        final int currentSchemaId;
        try {
            currentSchemaId = zookeeperClient.getCurrentSchemaId(tablePath);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    "Failed to get latest schema id of table " + tablePath, e);
        }
        return getSchemaById(tablePath, currentSchemaId);
    }

    public SchemaInfo getSchemaById(TablePath tablePath, int schemaId)
            throws SchemaNotExistException {
        Optional<SchemaInfo> optionalSchema;
        try {
            optionalSchema = zookeeperClient.getSchemaById(tablePath, schemaId);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Fail to get schema of %s for table %s", schemaId, tablePath), e);
        }
        if (optionalSchema.isPresent()) {
            return optionalSchema.get();
        } else {
            throw new SchemaNotExistException(
                    "Schema for table "
                            + tablePath
                            + " with schema id "
                            + schemaId
                            + " does not exist.");
        }
    }

    public boolean tableExists(TablePath tablePath) {
        // check the path of the table exists
        return uncheck(
                () -> zookeeperClient.tableExist(tablePath),
                String.format("Fail to check the table %s exist or not.", tablePath));
    }

    public long initWriterId() {
        return uncheck(
                zookeeperClient::getWriterIdAndIncrement, "Fail to get writer id from zookeeper");
    }

    private void rethrowIfIsNotNoNodeException(
            ThrowingRunnable<Exception> throwingRunnable, String exceptionMessage) {
        try {
            throwingRunnable.run();
        } catch (KeeperException.NoNodeException e) {
            // ignore
        } catch (Exception e) {
            throw new FlussRuntimeException(exceptionMessage, e);
        }
    }

    private static <T> T uncheck(Callable<T> callable, String errorMsg) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new FlussRuntimeException(errorMsg, e);
        }
    }

    private static void uncheck(RunnableWithException runnable, String errorMsg) {
        try {
            runnable.run();
        } catch (Exception e) {
            throw new FlussRuntimeException(errorMsg, e);
        }
    }

    private TableRegistration validateAndApplyUpdateProperties(
            TablePath tablePath, UpdateProperties updatePropertiesData) {
        TableRegistration tableReg = getTableRegistration(tablePath);
        Map<String, String> properties = new HashMap<>(tableReg.properties);
        Map<String, String> customProperties = new HashMap<>(tableReg.customProperties);
        // 1. set table properties.
        for (Map.Entry<String, String> setProperty :
                updatePropertiesData.getSetProperties().entrySet()) {
            validateSetTableProperty(setProperty.getKey());
            properties.put(setProperty.getKey(), setProperty.getValue());
        }
        // 2. reset table properties.
        for (String resetProperty : updatePropertiesData.getResetProperties()) {
            validateResetTableProperty(resetProperty, properties);
            properties.remove(resetProperty);
        }
        // 3. set custom properties.
        customProperties.putAll(updatePropertiesData.getSetCustomProperties());
        // 4. reset custom properties.
        for (String resetCustomProperty : updatePropertiesData.getResetCustomProperties()) {
            validateResetCustomProperty(resetCustomProperty, customProperties);
            customProperties.remove(resetCustomProperty);
        }

        return tableReg.copy(properties, customProperties);
    }

    private void validateSetTableProperty(String setKey) {
        if (!TABLE_OPTIONS_SUPPORT_ALTER.contains(setKey)) {
            throw new InvalidAlterTableException(
                    "Update table property: '" + setKey + "' is not supported yet.");
        }
    }

    private void validateResetTableProperty(String resetKey, Map<String, String> oldProperties) {
        if (!TABLE_OPTIONS_SUPPORT_ALTER.contains(resetKey)) {
            throw new UnsupportedOperationException(
                    "Reset table property: '" + resetKey + "' is not supported yet.");
        }

        if (!oldProperties.containsKey(resetKey)) {
            throw new InvalidAlterTableException(
                    "Reset table property: '"
                            + resetKey
                            + "' is not an exist table property in current table.");
        }
    }

    private void validateResetCustomProperty(
            String resetKey, Map<String, String> oldCustomProperties) {
        if (!oldCustomProperties.containsKey(resetKey)) {
            throw new InvalidAlterTableException(
                    "The custom property: '"
                            + resetKey
                            + "' to reset is not an exist custom property in current table.");
        }
    }
}
