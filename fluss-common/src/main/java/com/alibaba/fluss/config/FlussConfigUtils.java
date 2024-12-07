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

package com.alibaba.fluss.config;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.exception.InvalidConfigException;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.config.ConfigOptions.TABLE_DATALAKE_ENABLED;

/** Utilities of Fluss {@link ConfigOptions}. */
@Internal
public class FlussConfigUtils {

    public static final Map<String, ConfigOption<?>> TABLE_OPTIONS;
    public static final Map<String, ConfigOption<?>> CLIENT_OPTIONS;

    // table properties that support alter.
    public static final List<String> TABLE_OPTIONS_SUPPORT_ALTER =
            Collections.singletonList(TABLE_DATALAKE_ENABLED.key());

    static {
        TABLE_OPTIONS = extractConfigOptions("table.");
        CLIENT_OPTIONS = extractConfigOptions("client.");
    }

    /** Validate all table properties are known to Fluss and property values are valid. */
    public static void validateTableProperties(Map<String, String> tableProperties) {
        Configuration conf = Configuration.fromMap(tableProperties);
        for (String key : tableProperties.keySet()) {
            if (!TABLE_OPTIONS.containsKey(key)) {
                throw new InvalidConfigException(
                        String.format(
                                "'%s' is not a Fluss table property. Please use '.customProperty(..)' to set custom properties.",
                                key));
            }
            ConfigOption<?> option = TABLE_OPTIONS.get(key);
            validateOptionValue(conf, option);
        }

        if (conf.get(ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS) <= 0) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' must be greater than 0.",
                            ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS.key()));
        }
    }

    private static void validateOptionValue(ReadableConfig options, ConfigOption<?> option) {
        try {
            options.get(option);
        } catch (Throwable t) {
            throw new InvalidConfigException(
                    String.format(
                            "Invalid value for config '%s'. Reason: %s",
                            option.key(), t.getMessage()));
        }
    }

    private static Map<String, ConfigOption<?>> extractConfigOptions(String prefix) {
        Map<String, ConfigOption<?>> options = new HashMap<>();
        Field[] fields = ConfigOptions.class.getFields();
        // use Java reflection to collect all options matches the prefix
        for (Field field : fields) {
            if (!ConfigOption.class.isAssignableFrom(field.getType())) {
                continue;
            }
            try {
                ConfigOption<?> configOption = (ConfigOption<?>) field.get(null);
                if (configOption.key().startsWith(prefix)) {
                    options.put(configOption.key(), configOption);
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(
                        "Unable to extract ConfigOption fields from ConfigOptions class.", e);
            }
        }
        return options;
    }
}
