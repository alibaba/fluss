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

package com.alibaba.fluss.rpc;

import com.alibaba.fluss.cluster.ServerType;

// TODO: support MainThreadRpcGateway which ensures that all methods are executed on the main
//   thread.
/**
 * RPC gateway extension for servers which is able to get the API version of current API invocation.
 */
public abstract class RpcGatewayService implements RpcGateway {

    private final ThreadLocal<Short> currentApiVersion = new ThreadLocal<>();

    /** Returns the current API version of the RPC call. This method is thread-safe */
    public void setCurrentApiVersion(short apiVersion) {
        currentApiVersion.set(apiVersion);
    }

    /**
     * Returns the current API version of an RPC call. This method is thread-safe and can only be
     * accessed in RPC methods.
     */
    public short currentApiVersion() {
        Short version = currentApiVersion.get();
        if (version == null) {
            throw new IllegalStateException(
                    "No API version set. This method should only be called from within an RPC call.");
        } else {
            return version;
        }
    }

    /** Returns the provider type of this RPC gateway service. */
    public abstract ServerType providerType();

    /** The service name of the gateway used for logging. */
    public abstract String name();

    /** Shutdown the gateway service, release any resources. */
    public abstract void shutdown();
}
