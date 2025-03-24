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

package com.alibaba.fluss.flink.source;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.FlinkConnectorOptions;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.flink.source.lookup.FlinkAsyncLookupFunction;
import com.alibaba.fluss.flink.source.lookup.FlinkLookupFunction;
import com.alibaba.fluss.flink.source.lookup.LookupNormalizer;
import com.alibaba.fluss.flink.utils.FlinkConnectorOptionsUtils;
import com.alibaba.fluss.flink.utils.FlinkConversions;
import com.alibaba.fluss.flink.utils.PushdownUtils;
import com.alibaba.fluss.flink.utils.PushdownUtils.ValueConversion;
import com.alibaba.fluss.metadata.MergeEngineType;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsAggregatePushDown;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsRowLevelModificationScan;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.AggregateExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** Flink table source to scan Fluss data. */
public class FlinkTableSource
        implements ScanTableSource,
                SupportsProjectionPushDown,
                SupportsFilterPushDown,
                LookupTableSource,
                SupportsRowLevelModificationScan,
                SupportsLimitPushDown,
                SupportsAggregatePushDown {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkTableSource.class);
    private final TablePath tablePath;
    private final Configuration flussConfig;
    // output type before projection pushdown
    private final org.apache.flink.table.types.logical.RowType tableOutputType;
    // will be empty if no primary key
    private final int[] primaryKeyIndexes;
    // will be empty if no bucket key
    private final int[] bucketKeyIndexes;
    // will be empty if no partition key
    private final int[] partitionKeyIndexes;
    private final boolean streaming;
    private final boolean enableChangelog;
    private final FlinkConnectorOptionsUtils.StartupOptions startupOptions;

    // options for lookup source
    private final int lookupMaxRetryTimes;
    private final boolean lookupAsync;
    @Nullable private final LookupCache cache;

    private final long scanPartitionDiscoveryIntervalMs;
    private final boolean isDataLakeEnabled;
    @Nullable private final MergeEngineType mergeEngineType;

    // output type after projection pushdown
    private LogicalType producedDataType;

    // projection push down
    @Nullable private int[] projectedFields;

    // track selected metadata columns
    @Nullable private int[] selectedMetadataFields;

    @Nullable private GenericRowData singleRowFilter;

    // whether the scan is for row-level modification
    @Nullable private RowLevelModificationType modificationScanType;

    // count(*) push down
    protected boolean selectRowCount = false;

    private long limit = -1;

    public FlinkTableSource(
            TablePath tablePath,
            Configuration flussConfig,
            org.apache.flink.table.types.logical.RowType tableOutputType,
            int[] primaryKeyIndexes,
            int[] bucketKeyIndexes,
            int[] partitionKeyIndexes,
            boolean streaming,
            FlinkConnectorOptionsUtils.StartupOptions startupOptions,
            int lookupMaxRetryTimes,
            boolean lookupAsync,
            @Nullable LookupCache cache,
            long scanPartitionDiscoveryIntervalMs,
            boolean isDataLakeEnabled,
            @Nullable MergeEngineType mergeEngineType,
            boolean enableChangelog) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.tableOutputType = tableOutputType;
        this.producedDataType = tableOutputType;
        this.primaryKeyIndexes = primaryKeyIndexes;
        this.bucketKeyIndexes = bucketKeyIndexes;
        this.partitionKeyIndexes = partitionKeyIndexes;
        this.streaming = streaming;
        this.startupOptions = checkNotNull(startupOptions, "startupOptions must not be null");

        this.lookupMaxRetryTimes = lookupMaxRetryTimes;
        this.lookupAsync = lookupAsync;
        this.cache = cache;

        this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
        this.isDataLakeEnabled = isDataLakeEnabled;
        this.mergeEngineType = mergeEngineType;
        this.enableChangelog = enableChangelog;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        if (!streaming) {
            return ChangelogMode.insertOnly();
        } else {
            if (hasPrimaryKey()) {
                // pk table
                if (mergeEngineType == MergeEngineType.FIRST_ROW) {
                    return ChangelogMode.insertOnly();
                } else {
                    return ChangelogMode.all();
                }
            } else {
                // append only
                return ChangelogMode.insertOnly();
            }
        }
    }

    private boolean hasPrimaryKey() {
        return primaryKeyIndexes.length > 0;
    }

    private boolean isPartitioned() {
        return partitionKeyIndexes.length > 0;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        // handle single row filter scan
        if (singleRowFilter != null || limit > 0 || selectRowCount) {
            Collection<RowData> results;
            if (singleRowFilter != null) {
                results =
                        PushdownUtils.querySingleRow(
                                singleRowFilter,
                                tablePath,
                                flussConfig,
                                tableOutputType,
                                primaryKeyIndexes,
                                lookupMaxRetryTimes,
                                projectedFields);
            } else if (limit > 0) {
                results =
                        PushdownUtils.limitScan(
                                tablePath, flussConfig, tableOutputType, projectedFields, limit);
            } else {
                results =
                        Collections.singleton(
                                GenericRowData.of(
                                        PushdownUtils.countLogTable(tablePath, flussConfig)));
            }

            TypeInformation<RowData> resultTypeInfo =
                    scanContext.createTypeInformation(producedDataType);
            return new DataStreamScanProvider() {
                @Override
                public DataStream<RowData> produceDataStream(
                        ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                    return execEnv.fromCollection(results, resultTypeInfo);
                }

                @Override
                public boolean isBounded() {
                    return true;
                }
            };
        }

        RowType flussRowType = FlinkConversions.toFlussRowType(tableOutputType);

        if (enableChangelog) {
            // STEP 1: Determine which metadata fields are needed
            if (selectedMetadataFields == null) {
                // For SELECT * or when nothing is explicitly selected, include all metadata fields
                if (projectedFields == null) {
                    this.selectedMetadataFields =
                            new int[] {0, 1, 2}; // All metadata fields for SELECT *
                    LOG.debug("SELECT * case - including all metadata fields");
                } else if (projectedFields.length == 0) {
                    // Handle metadata-only queries - for cases like SELECT _change_type,
                    // _log_offset, _commit_timestamp
                    this.selectedMetadataFields = new int[] {0, 1, 2}; // All metadata fields
                    LOG.debug("Detected metadata-only query - including all metadata fields");
                } else {
                    // For queries with only physical fields like SELECT a,b,c - explicitly set
                    // EMPTY metadata
                    this.selectedMetadataFields = new int[0]; // No metadata fields
                    LOG.debug("Only physical fields selected - setting empty metadata fields");
                }
            } else if (selectedMetadataFields.length == 0
                    && projectedFields != null
                    && projectedFields.length == 0) {
                // Edge case: Empty arrays for both physical and metadata, but changelog mode
                // This might be a metadata-only query that was incorrectly parsed
                this.selectedMetadataFields = new int[] {0, 1, 2};
                LOG.debug(
                        "Edge case: Empty projections in changelog mode - defaulting to all metadata fields");
            }

            // STEP 2: Set up physical field projection
            List<String> fieldNames = tableOutputType.getFieldNames();
            List<String> projectedFieldNames = new ArrayList<>();

            if (projectedFields == null) {
                // If projectedFields is null, it means "SELECT *" - include all physical fields
                LOG.debug("SELECT * - including all {} physical fields", fieldNames.size() - 3);
                for (int i = 3; i < fieldNames.size(); i++) {
                    projectedFieldNames.add(fieldNames.get(i));
                }
            } else if (projectedFields.length > 0) {
                // Normal projection case
                LOG.debug("Projecting {} specific physical fields", projectedFields.length);
                for (int idx : projectedFields) {
                    // Account for metadata fields in the index
                    projectedFieldNames.add(fieldNames.get(idx + 3));
                }
            } else {
                // No physical fields selected - might be metadata-only query
                LOG.debug("No physical fields in projection - likely metadata-only query");
            }

            // Apply physical field projection if we have any fields to project
            if (!projectedFieldNames.isEmpty()) {
                flussRowType = flussRowType.project(projectedFieldNames);
                LOG.debug(
                        "Projected row type has {} fields: {}",
                        flussRowType.getFieldCount(),
                        flussRowType.getFieldNames());
            } else if (selectedMetadataFields.length > 0) {
                // if only metadata fields asked without physical fields
                throw new UnsupportedOperationException(
                        "Queries selecting only metadata columns are not supported. "
                                + "You must include at least one physical field in your query.\n\n"
                                + "For example, instead of:\n"
                                + "    SELECT _change_type, _log_offset, _commit_timestamp FROM changelog_test$changelog\n\n"
                                + "Use:\n"
                                + "    SELECT _change_type, _log_offset, _commit_timestamp, phycialField FROM changelog_test$changelog");
            }
        } else if (!enableChangelog && projectedFields != null) {
            // For non-changelog tables, continue using index-based projection
            flussRowType = flussRowType.project(projectedFields);
        }

        OffsetsInitializer offsetsInitializer;
        switch (startupOptions.startupMode) {
            case EARLIEST:
                offsetsInitializer = OffsetsInitializer.earliest();
                break;
            case LATEST:
                offsetsInitializer = OffsetsInitializer.latest();
                break;
            case FULL:
                offsetsInitializer = OffsetsInitializer.initial();
                break;
            case TIMESTAMP:
                offsetsInitializer =
                        OffsetsInitializer.timestamp(startupOptions.startupTimestampMs);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported startup mode: " + startupOptions.startupMode);
        }

        FlinkSource source =
                new FlinkSource(
                        flussConfig,
                        tablePath,
                        hasPrimaryKey(),
                        isPartitioned(),
                        flussRowType,
                        projectedFields,
                        offsetsInitializer,
                        scanPartitionDiscoveryIntervalMs,
                        streaming,
                        enableChangelog,
                        selectedMetadataFields);

        if (!streaming) {
            // return a bounded source provide to make planner happy,
            // but this should throw exception when used to create source
            return new SourceProvider() {
                @Override
                public boolean isBounded() {
                    return true;
                }

                @Override
                public Source<RowData, ?, ?> createSource() {
                    if (modificationScanType != null) {
                        throw new UnsupportedOperationException(
                                "Currently, Fluss table only supports "
                                        + modificationScanType
                                        + " statement with conditions on primary key.");
                    }
                    if (!isDataLakeEnabled) {
                        throw new UnsupportedOperationException(
                                "Currently, Fluss only support queries on table with datalake enabled or point queries on primary key when it's in batch execution mode.");
                    }
                    return source;
                }
            };
        } else {
            return SourceProvider.of(source);
        }
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        LookupNormalizer lookupNormalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        context.getKeys(),
                        primaryKeyIndexes,
                        bucketKeyIndexes,
                        partitionKeyIndexes,
                        tableOutputType,
                        projectedFields);
        if (lookupAsync) {
            AsyncLookupFunction asyncLookupFunction =
                    new FlinkAsyncLookupFunction(
                            flussConfig,
                            tablePath,
                            tableOutputType,
                            lookupMaxRetryTimes,
                            lookupNormalizer,
                            projectedFields);
            if (cache != null) {
                return PartialCachingAsyncLookupProvider.of(asyncLookupFunction, cache);
            } else {
                return AsyncLookupFunctionProvider.of(asyncLookupFunction);
            }
        } else {
            LookupFunction lookupFunction =
                    new FlinkLookupFunction(
                            flussConfig,
                            tablePath,
                            tableOutputType,
                            lookupMaxRetryTimes,
                            lookupNormalizer,
                            projectedFields);
            if (cache != null) {
                return PartialCachingLookupProvider.of(lookupFunction, cache);
            } else {
                return LookupFunctionProvider.of(lookupFunction);
            }
        }
    }

    @Override
    public DynamicTableSource copy() {
        FlinkTableSource source =
                new FlinkTableSource(
                        tablePath,
                        flussConfig,
                        tableOutputType,
                        primaryKeyIndexes,
                        bucketKeyIndexes,
                        partitionKeyIndexes,
                        streaming,
                        startupOptions,
                        lookupMaxRetryTimes,
                        lookupAsync,
                        cache,
                        scanPartitionDiscoveryIntervalMs,
                        isDataLakeEnabled,
                        mergeEngineType,
                        enableChangelog);
        source.producedDataType = producedDataType;
        source.projectedFields = projectedFields;
        source.selectedMetadataFields = selectedMetadataFields;
        source.singleRowFilter = singleRowFilter;
        source.modificationScanType = modificationScanType;
        return source;
    }

    @Override
    public String asSummaryString() {
        return "FlussTableSource";
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        if (enableChangelog) {
            List<Integer> physicalFieldIndices = new ArrayList<>();
            List<Integer> metadataFieldIndices = new ArrayList<>();

            for (int[] field : projectedFields) {
                int index = field[0];
                if (index < 3) { // First 3 columns are metadata in changelog tables
                    metadataFieldIndices.add(index);
                } else {
                    // Adjust index for physical columns (subtract metadata column count)
                    physicalFieldIndices.add(index - 3);
                }
            }

            // Set projected fields (may be empty!)
            this.projectedFields = physicalFieldIndices.stream().mapToInt(i -> i).toArray();
            this.selectedMetadataFields = metadataFieldIndices.stream().mapToInt(i -> i).toArray();
        } else {
            // Original implementation for non-changelog tables
            this.projectedFields =
                    Arrays.stream(projectedFields).mapToInt(value -> value[0]).toArray();
        }
        this.producedDataType = producedDataType.getLogicalType();
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        // only apply pk equal filters when all the condition satisfied:
        // (1) batch execution mode,
        // (2) default (full) startup mode,
        // (3) the table is a pk table,
        // (4) all filters are pk field equal expression
        if (streaming
                || startupOptions.startupMode != FlinkConnectorOptions.ScanStartupMode.FULL
                || !hasPrimaryKey()
                || filters.size() != primaryKeyIndexes.length) {
            return Result.of(Collections.emptyList(), filters);
        }

        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        List<ResolvedExpression> remainingFilters = new ArrayList<>();
        Map<Integer, LogicalType> primaryKeyTypes = getPrimaryKeyTypes();
        List<PushdownUtils.FieldEqual> fieldEquals =
                PushdownUtils.extractFieldEquals(
                        filters,
                        primaryKeyTypes,
                        acceptedFilters,
                        remainingFilters,
                        ValueConversion.FLINK_INTERNAL_VALUE);
        int[] keyRowProjection = getKeyRowProjection();
        HashSet<Integer> visitedPkFields = new HashSet<>();
        GenericRowData lookupRow = new GenericRowData(primaryKeyIndexes.length);
        for (PushdownUtils.FieldEqual fieldEqual : fieldEquals) {
            lookupRow.setField(keyRowProjection[fieldEqual.fieldIndex], fieldEqual.equalValue);
            visitedPkFields.add(fieldEqual.fieldIndex);
        }
        // if not all primary key fields are in condition, we skip to pushdown
        if (!visitedPkFields.equals(primaryKeyTypes.keySet())) {
            return Result.of(Collections.emptyList(), filters);
        }
        singleRowFilter = lookupRow;
        return Result.of(acceptedFilters, remainingFilters);
    }

    @Override
    public RowLevelModificationScanContext applyRowLevelModificationScan(
            RowLevelModificationType rowLevelModificationType,
            @Nullable RowLevelModificationScanContext rowLevelModificationScanContext) {
        modificationScanType = rowLevelModificationType;
        return null;
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public boolean applyAggregates(
            List<int[]> groupingSets,
            List<AggregateExpression> aggregateExpressions,
            DataType dataType) {
        // Only supports 'select count(*)/count(1) from source' for log table now.
        if (streaming
                || aggregateExpressions.size() != 1
                || hasPrimaryKey()
                || groupingSets.size() > 1
                || (groupingSets.size() == 1 && groupingSets.get(0).length > 0)) {
            return false;
        }

        FunctionDefinition functionDefinition = aggregateExpressions.get(0).getFunctionDefinition();
        if (!(functionDefinition
                        .getClass()
                        .getCanonicalName()
                        .equals(
                                "org.apache.flink.table.planner.functions.aggfunctions.CountAggFunction")
                || functionDefinition
                        .getClass()
                        .getCanonicalName()
                        .equals(
                                "org.apache.flink.table.planner.functions.aggfunctions.Count1AggFunction"))) {
            return false;
        }
        selectRowCount = true;
        this.producedDataType = dataType.getLogicalType();
        return true;
    }

    private Map<Integer, LogicalType> getPrimaryKeyTypes() {
        Map<Integer, LogicalType> pkTypes = new HashMap<>();
        for (int index : primaryKeyIndexes) {
            pkTypes.put(index, tableOutputType.getTypeAt(index));
        }
        return pkTypes;
    }

    // projection from pk_field_index to index_in_pk
    private int[] getKeyRowProjection() {
        int[] projection = new int[tableOutputType.getFieldCount()];
        for (int i = 0; i < primaryKeyIndexes.length; i++) {
            projection[primaryKeyIndexes[i]] = i;
        }
        return projection;
    }

    @Nullable
    public int[] getSelectedMetadataFields() {
        return selectedMetadataFields;
    }
}
