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

import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.protocol.ApiMethod;
import com.alibaba.fluss.rpc.protocol.RequestType;
import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * An RPC request message sent from client to server which encapsulating the API key, API version
 * and message.
 */
public final class FlussRequest implements RpcRequest {
    public static final FlussRequest SHUTDOWN_REQUEST =
            new FlussRequest((short) -1, (short) -1, -1, null);

    private final short apiKey;
    private final short apiVersion;
    private final int requestId;
    private final ApiMethod apiMethod;
    private final ApiMessage message;
    private final ByteBuf buffer;
    private final String listenerName;
    private final FlussPrincipal principal;
    private final ChannelHandlerContext channelContext;

    // the time when the request is received by server
    private final long startTimeMs;

    public FlussRequest(
            short apiKey,
            short apiVersion,
            int requestId,
            ApiMethod apiMethod,
            ApiMessage message,
            ByteBuf buffer,
            String listenerName,
            FlussPrincipal principal,
            ChannelHandlerContext channelContext) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.requestId = requestId;
        this.apiMethod = apiMethod;
        this.message = message;
        this.buffer = checkNotNull(buffer);
        this.listenerName = listenerName;
        this.principal = principal;
        this.channelContext = channelContext;
        this.startTimeMs = System.currentTimeMillis();
    }

    // only used for the SHUTDOWN_REQUEST instance.
    FlussRequest(short apiKey, short apiVersion, int requestId, ApiMethod apiMethod) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.requestId = requestId;
        this.apiMethod = apiMethod;
        this.message = null;
        this.buffer = null;
        this.listenerName = null;
        this.principal = null;
        this.channelContext = null;
        this.startTimeMs = System.currentTimeMillis();
    }

    @Override
    public RequestType getRequestType() {
        return RequestType.FLUSS;
    }

    public short getApiKey() {
        return apiKey;
    }

    public short getApiVersion() {
        return apiVersion;
    }

    public int getRequestId() {
        return requestId;
    }

    public ApiMethod getApiMethod() {
        return apiMethod;
    }

    public ApiMessage getMessage() {
        return message;
    }

    public void releaseBuffer() {
        if (message.isLazilyParsed()) {
            buffer.release();
        }
    }

    public ChannelHandlerContext getChannelContext() {
        return channelContext;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public String getListenerName() {
        return listenerName;
    }

    @Override
    public String toString() {
        return "RpcRequest{"
                + "apiKey="
                + apiKey
                + ", apiVersion="
                + apiVersion
                + ", requestId="
                + requestId
                + ", listenerName="
                + listenerName
                + '}';
    }
}
