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

package com.alibaba.fluss.flink.utils;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * This class overrides the {@link FlinkConversions} from {@code fluss-flink-common} to adapt to
 * newer Flink versions where {@link
 * org.apache.flink.table.catalog.CatalogTable#of(org.apache.flink.table.api.Schema,
 * java.lang.String, java.util.List, java.util.Map)} is no longer supported.
 */
public class CatalogTableUtils {
    public static CatalogTable toCatalogTable(
            Schema schema,
            @Nullable String comment,
            List<String> partitionKeys,
            Map<String, String> options) {
        return CatalogTable.of(schema, comment, partitionKeys, options);
    }
}
