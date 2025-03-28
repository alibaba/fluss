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

import com.alibaba.fluss.metrics.Histogram;

import org.junit.jupiter.api.BeforeEach;

import static org.mockito.Mockito.mock;

public class FlinkHistogramTest extends WrapperMetricsTestSuite<Histogram, FlinkHistogram> {

    private FlinkHistogram flinkHistogram;
    private Histogram histogram;

    @BeforeEach
    void setUp() {
        histogram = mock(Histogram.class);
        flinkHistogram = new FlinkHistogram(histogram);
    }

    @Override
    protected Histogram getWrappedMetricInstance() throws Exception {
        return histogram;
    }

    @Override
    protected FlinkHistogram getMetricInstance() throws Exception {
        return flinkHistogram;
    }
}
