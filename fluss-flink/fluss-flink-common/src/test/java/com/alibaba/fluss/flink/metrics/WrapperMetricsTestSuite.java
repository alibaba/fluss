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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockingDetails;
import org.mockito.invocation.Invocation;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.mockito.Mockito.mockingDetails;

public abstract class WrapperMetricsTestSuite<T, E> {

    @Test
    void testPublicMethods() throws Exception {
        T wrapped = getWrappedMetricInstance();

        E object = getMetricInstance();

        Method[] methods = object.getClass().getMethods();

        for (Method method : methods) {
            if (method.getDeclaringClass().equals(object.getClass())) {
                Object[] arguments = createParams(method);
                method.invoke(object, arguments);
                MockingDetails mockingDetails = mockingDetails(wrapped);

                boolean invoked =
                        mockingDetails.getInvocations().stream()
                                .anyMatch(iv -> invocationFilter(method, arguments, iv));

                Assertions.assertThat(invoked).isTrue();
            }
        }
    }

    private static boolean invocationFilter(Method method, Object[] arguments, Invocation iv) {
        return Arrays.equals(arguments, iv.getArguments())
                && iv.getMethod().getName().equals(method.getName());
    }

    private static Object[] createParams(Method method) {
        Object[] params = new Object[] {};
        if (method.getParameterTypes().length > 0) {
            if (method.getParameterTypes()[0].equals(double.class)) {
                params = new Object[] {0.0d};
            } else {
                params = new Object[] {0L};
            }
        }
        return params;
    }

    protected abstract T getWrappedMetricInstance() throws Exception;

    protected abstract E getMetricInstance() throws Exception;
}
