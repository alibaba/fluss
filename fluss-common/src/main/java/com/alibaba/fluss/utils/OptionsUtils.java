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

package com.alibaba.fluss.utils;

import java.util.Map;
import java.util.stream.Collectors;

/** Utility class for Options related helper functions. */
public class OptionsUtils {

    public static Map<String, String> convertToPropertiesWithPrefixKey(
            Map<String, String> configParams, final String prefixKey) {
        return configParams.keySet().stream()
                .filter(k -> k.startsWith(prefixKey))
                .collect(Collectors.toMap(k -> k.substring(prefixKey.length()), configParams::get));
    }
}
