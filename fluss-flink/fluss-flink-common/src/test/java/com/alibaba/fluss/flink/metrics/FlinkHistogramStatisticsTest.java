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

import org.apache.flink.metrics.HistogramStatistics;
import org.junit.jupiter.api.BeforeEach;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlinkHistogramStatisticsTest
        extends WrapperMetricsTestSuite<
                com.alibaba.fluss.metrics.HistogramStatistics, HistogramStatistics> {

    private com.alibaba.fluss.metrics.HistogramStatistics wrapped;
    private HistogramStatistics wrapper;

    @BeforeEach
    void setUp() {
        Histogram histogram = mock(Histogram.class);
        wrapped = mock(com.alibaba.fluss.metrics.HistogramStatistics.class);
        when(histogram.getStatistics()).thenReturn(wrapped);

        wrapper = new FlinkHistogram(histogram).getStatistics();
    }

    @Override
    protected com.alibaba.fluss.metrics.HistogramStatistics getWrappedMetricInstance()
            throws Exception {
        return wrapped;
    }

    @Override
    protected HistogramStatistics getMetricInstance() throws Exception {
        return wrapper;
    }
}
