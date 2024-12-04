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

package com.alibaba.fluss.connector.flink.lakehouse.paimon;

import com.alibaba.fluss.connector.flink.lakehouse.LakeCatalog;

import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.FlinkFileIOLoader;
import org.apache.paimon.options.Options;

import java.util.Map;

/** A paimon lake catalog to delegate the operations on lake table. */
public class PaimonLakeCatalog implements LakeCatalog {

    private final FlinkCatalog paimonFlinkCatalog;

    public PaimonLakeCatalog(
            String catalogName, Map<String, String> catalogProperties, ClassLoader classLoader) {
        CatalogContext catalogContext =
                CatalogContext.create(
                        Options.fromMap(catalogProperties), null, new FlinkFileIOLoader());
        paimonFlinkCatalog =
                FlinkCatalogFactory.createCatalog(catalogName, catalogContext, classLoader);
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath objectPath)
            throws TableNotExistException, CatalogException {
        return paimonFlinkCatalog.getTable(objectPath);
    }
}
