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

package com.alibaba.fluss.rpc.netty.client;

import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.messages.AuthenticateRequest;

import java.util.concurrent.CompletableFuture;

/**
 * A callback interface used by {@link NettyAuthenticationHandler} to notify authentication events.
 */
public interface AuthenticatorHandlerCallback {

    /** Called when an authentication token is generated. */
    CompletableFuture<ApiMessage> onAuthenticating(AuthenticateRequest request);

    /** Called when authentication failed, this will close the connection. */
    void onAuthenticateFailure(Throwable cause);

    /**
     * Called when the authentication process is successfully completed.
     *
     * <p>After this method is invoked, the connection is considered authenticated, and subsequent
     * user requests can be safely sent over the channel.
     */
    void onAuthenticateSuccess();
}
