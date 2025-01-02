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

package com.alibaba.fluss.server.kv.mergeengine;

import com.alibaba.fluss.record.KvRecord;

/** Merge engine wrapper for table. */
public interface MergeEngineWrapper {

    /**
     * Write record to table.
     *
     * @param kvRecord
     * @throws Exception
     */
    void writeRecord(KvRecord kvRecord) throws Exception;

    /**
     * Track the appended record count.
     *
     * @return
     */
    int getAppendedRecordCount();

    /**
     * Track the log offset.
     *
     * @return
     */
    long getLogOffset();
}
