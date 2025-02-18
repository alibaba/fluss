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

package com.alibaba.fluss.rpc.netty.server;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.rpc.RpcGatewayService;
import com.alibaba.fluss.rpc.protocol.ApiManager;
import com.alibaba.fluss.rpc.protocol.NetworkProtocolPlugin;
import com.alibaba.fluss.security.auth.AuthenticatorLoader;
import com.alibaba.fluss.security.auth.PlainTextAuthenticationPlugin;
import com.alibaba.fluss.security.auth.ServerAuthenticator;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandler;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/** Build-in protocol plugin for Fluss. */
public class FlussProtocolPlugin implements NetworkProtocolPlugin {
    private final Map<String, Supplier<ServerAuthenticator>> authenticatorSuppliers;
    private final ApiManager apiManager;
    private final long maxIdleTimeSeconds;
    private final List<String> listeners;

    public FlussProtocolPlugin(Configuration conf, ServerType serverType, List<String> listeners) {
        authenticatorSuppliers = AuthenticatorLoader.loadServerAuthenticatorSuppliers(conf);
        apiManager = new ApiManager(serverType);
        maxIdleTimeSeconds = conf.get(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME).getSeconds();
        this.listeners = listeners;
    }

    @Override
    public String name() {
        return "FLUSS";
    }

    @Override
    public List<String> listenerNames(Configuration conf) {
        return listeners;
    }

    @Override
    public ChannelHandler createChannelHandler(
            RequestChannel[] requestChannels, String listenerName) {
        return new ServerChannelInitializer(
                requestChannels,
                apiManager,
                listenerName,
                Optional.ofNullable(authenticatorSuppliers.get(listenerName))
                        .orElse(PlainTextAuthenticationPlugin.PlainTextServerAuthenticator::new),
                maxIdleTimeSeconds);
    }

    @Override
    public RequestHandler<?> createRequestHandler(
            RpcGatewayService service, RequestsMetrics requestsMetrics) {
        return new FlussRequestHandler(service, requestsMetrics);
    }

    @VisibleForTesting
    ApiManager getApiManager() {
        return apiManager;
    }
}
