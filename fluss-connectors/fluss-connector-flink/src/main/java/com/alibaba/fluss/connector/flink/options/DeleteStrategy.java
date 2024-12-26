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

package com.alibaba.fluss.connector.flink.options;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/** The strategy when fluss sink receives delete data. */
@PublicEvolving
public enum DeleteStrategy implements Serializable {

    /**
     * Ignore -U and -D. This is applicable for scenarios where users only need to insert or update
     * data without the need to delete data.
     */
    IGNORE_DELETE,

    /**
     * Operate normally based on PK + rowkind, suitable for scenarios that do not involve localized
     * updates. The Flink framework operates according to the Flink SQL Changelog working
     * principles, not ignoring delete operations, and executes update operations by first deleting
     * data then inserting, to ensure data accuracy.
     */
    CHANGELOG_STANDARD;
}
