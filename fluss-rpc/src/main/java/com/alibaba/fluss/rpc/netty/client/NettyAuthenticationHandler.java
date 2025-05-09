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

import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.RetriableAuthenticationException;
import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.messages.AuthenticateRequest;
import com.alibaba.fluss.rpc.messages.AuthenticateResponse;
import com.alibaba.fluss.security.auth.ClientAuthenticator;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of the channel handler to send authenticator. The client handler is not shared,
 * there is a client handler instance for each channel connection.
 */
public class NettyAuthenticationHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(NettyAuthenticationHandler.class);
    private static final Random RANDOM = new Random();
    private static final long BASE_BACKOFF_MS = 500L;
    private static final long MAX_BACKOFF_MS = 60000L;
    // Jitter factor for randomization
    private static final double JITTER_FACTOR = 0.2;
    private static final int MAX_RETRY_AUTH_NUM = 10;

    private final AuthenticatorHandlerCallback callback;
    private final ClientAuthenticator authenticator;

    private int retryAuthNum = 0;
    private ChannelHandlerContext context;
    private AuthenticateRequest pendingRequest;

    public NettyAuthenticationHandler(
            AuthenticatorHandlerCallback callback, ClientAuthenticator authenticator) {
        this.callback = callback;
        this.authenticator = authenticator;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        // send initial authentication token.
        context = ctx;
        LOG.debug("Begin to authenticate with protocol {}", authenticator.protocol());
        generateTokenAndSendAuthenticate(new byte[0]);
    }

    private void generateTokenAndSendAuthenticate(byte[] challenge) {
        try {
            if (!authenticator.isCompleted()) {
                byte[] token = authenticator.authenticate(challenge);
                if (token != null) {
                    AuthenticateRequest request =
                            new AuthenticateRequest()
                                    .setToken(token)
                                    .setProtocol(authenticator.protocol());
                    pendingRequest = request;
                    sendAuthenticateRequest(request);
                    return;
                }
            }

            assert authenticator.isCompleted();
            callback.onAuthenticateSuccess();
        } catch (Exception e) {
            LOG.warn(
                    "Authentication failed when authenticating challenge: {}",
                    new String(challenge),
                    e);
            callback.onAuthenticateFailure(
                    new FlussRuntimeException(
                            "Authentication failed when authenticating challenge", e));
        }
    }

    private void sendAuthenticateRequest(AuthenticateRequest request) {
        callback.onAuthenticating(request).whenComplete(this::handleAuthenticateResponse);
    }

    private void handleAuthenticateResponse(ApiMessage response, Throwable cause) {
        if (cause != null) {
            if (cause instanceof RetriableAuthenticationException
                    && retryAuthNum < MAX_RETRY_AUTH_NUM) {
                LOG.warn(
                        "Authentication failed, retrying {} of {} times",
                        retryAuthNum,
                        MAX_RETRY_AUTH_NUM,
                        cause);
                retryAuthNum++;
                context.executor()
                        .schedule(
                                () -> sendAuthenticateRequest(pendingRequest),
                                calculateBackOff(),
                                TimeUnit.MILLISECONDS);
            } else {
                LOG.warn("Authentication failed, and won't retry.", cause);
                callback.onAuthenticateFailure(cause);
            }
            return;
        }
        if (!(response instanceof AuthenticateResponse)) {
            callback.onAuthenticateFailure(
                    new IllegalStateException("Unexpected response type " + response.getClass()));
            return;
        }

        AuthenticateResponse authenticateResponse = (AuthenticateResponse) response;
        if (authenticateResponse.hasChallenge()) {
            generateTokenAndSendAuthenticate(((AuthenticateResponse) response).getChallenge());
        } else if (authenticator.isCompleted()) {
            callback.onAuthenticateSuccess();
        } else {
            callback.onAuthenticateFailure(
                    new IllegalStateException(
                            new IllegalStateException(
                                    "client authenticator is not completed while server generate no challenge.")));
        }
    }

    private long calculateBackOff() {
        // Exponential backoff with jitter
        long backOff = BASE_BACKOFF_MS * (1L << retryAuthNum);
        backOff = Math.min(backOff, MAX_BACKOFF_MS); // Ensure it does not exceed maxBackOffMs
        long jitter = (long) (backOff * JITTER_FACTOR * RANDOM.nextDouble());
        return backOff + jitter;
    }
}
