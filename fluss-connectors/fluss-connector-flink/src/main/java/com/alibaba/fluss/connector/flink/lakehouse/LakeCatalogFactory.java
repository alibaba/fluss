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

package com.alibaba.fluss.connector.flink.lakehouse;

import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.factories.Factory;
import com.alibaba.fluss.factories.FactoryUtils;
import com.alibaba.fluss.lakehouse.LakeStorageInfo;

import javax.annotation.Nullable;

import java.util.Map;

/** Factory to create {@link LakeCatalog}. */
public abstract class LakeCatalogFactory implements Factory {

    protected abstract LakeCatalog create(
            String catalogName, Map<String, String> catalogProperties, ClassLoader classLoader);

    private static volatile @Nullable LakeCatalog lakeCatalog;

    /** Get or create a {@link LakeCatalog} instance. */
    public static LakeCatalog getOrCreate(
            Admin admin, String catalogName, ClassLoader classLoader) {
        if (lakeCatalog == null) {
            synchronized (LakeCatalogFactory.class) {
                if (lakeCatalog == null) {
                    lakeCatalog = create(admin, catalogName, classLoader);
                }
            }
        }
        return lakeCatalog;
    }

    /** Create a {@link LakeCatalog} instance. */
    static LakeCatalog create(Admin admin, String catalogName, ClassLoader classLoader) {
        String lakeStorage = null;
        try {
            LakeStorageInfo lakeStorageInfo = admin.describeLakeStorage().get();
            lakeStorage = lakeStorageInfo.getLakeStorage();
            LakeCatalogFactory lakeCatalogFactory =
                    FactoryUtils.discoverFactory(
                            classLoader, LakeCatalogFactory.class, lakeStorage);
            return lakeCatalogFactory.create(
                    catalogName, lakeStorageInfo.getCatalogProperties(), classLoader);
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to init " + lakeStorage + " lake catalog.", e);
        }
    }
}
