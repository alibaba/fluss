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

package com.alibaba.fluss.flink.metrics;

import com.alibaba.fluss.metrics.Meter;

import org.junit.jupiter.api.BeforeEach;

import static org.mockito.Mockito.mock;

public class FlinkMeterTest extends WrapperMetricsTestSuite<Meter, FlinkMeter> {

    private FlinkMeter flinkMeter;
    private Meter meter;

    @BeforeEach
    void setUp() {
        meter = mock(Meter.class);
        flinkMeter = new FlinkMeter(meter);
    }

    @Override
    protected Meter getWrappedMetricInstance() throws Exception {
        return meter;
    }

    @Override
    protected FlinkMeter getMetricInstance() throws Exception {
        return flinkMeter;
    }
}
