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

package com.alibaba.fluss.kafka;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaConfigsTest {
    @Test
    public void testFromMap() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put(ConfigOptions.KAFKA_ENABLED.key(), "true");
        map.put(ConfigOptions.KAFKA_PORT.key(), "9093");
        map.put(ConfigOptions.KAFKA_DATABASE.key(), "fluss");
        Configuration configuration = Configuration.fromMap(map);

        assertThat(configuration.getBoolean(ConfigOptions.KAFKA_ENABLED)).isTrue();
        assertThat(configuration.getInt(ConfigOptions.KAFKA_PORT)).isEqualTo(9093);
        assertThat(configuration.getString(ConfigOptions.KAFKA_DATABASE)).isEqualTo("fluss");
    }

    @Test
    public void testFromDefault() throws Exception {
        Configuration configuration = Configuration.fromMap(new HashMap<>());
        assertThat(configuration.getBoolean(ConfigOptions.KAFKA_ENABLED)).isFalse();
        assertThat(configuration.getInt(ConfigOptions.KAFKA_PORT)).isEqualTo(9092);
        assertThat(configuration.getString(ConfigOptions.KAFKA_DATABASE)).isEqualTo("_kafka");
    }
}
