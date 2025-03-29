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

import com.alibaba.fluss.exception.AuthenticationException;
import com.alibaba.fluss.record.send.Send;
import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.messages.AuthenticateRequest;
import com.alibaba.fluss.rpc.messages.AuthenticateResponse;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.rpc.protocol.ApiKeys;
import com.alibaba.fluss.rpc.protocol.ApiManager;
import com.alibaba.fluss.rpc.protocol.ApiMethod;
import com.alibaba.fluss.rpc.protocol.MessageCodec;
import com.alibaba.fluss.security.auth.ServerAuthenticator;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelFutureListener;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandler;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.timeout.IdleState;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateEvent;
import com.alibaba.fluss.utils.ExceptionUtils;
import com.alibaba.fluss.utils.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.fluss.rpc.protocol.MessageCodec.encodeErrorResponse;
import static com.alibaba.fluss.rpc.protocol.MessageCodec.encodeServerFailure;
import static com.alibaba.fluss.rpc.protocol.MessageCodec.encodeSuccessResponse;

/** Implementation of the channel handler to process inbound requests for RPC server. */
@ChannelHandler.Sharable
public final class NettyServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(NettyServerHandler.class);

    private final RequestChannel[] requestChannels;
    private final int numChannels;
    private final ApiManager apiManager;
    private final String listenerName;
    private final ServerAuthenticator authenticator;

    private ConnectionState state;

    public NettyServerHandler(
            RequestChannel[] requestChannels,
            ApiManager apiManager,
            String listenerName,
            ServerAuthenticator authenticator) {
        this.requestChannels = requestChannels;
        this.numChannels = requestChannels.length;
        this.apiManager = apiManager;
        this.listenerName = listenerName;
        this.authenticator = authenticator;
        this.state =
                authenticator.isComplete() ? ConnectionState.READY : ConnectionState.AUTHENTICATING;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buffer = (ByteBuf) msg;
        int frameLength = buffer.readInt();
        short apiKey = buffer.readShort();
        short apiVersion = buffer.readShort();
        int requestId = buffer.readInt();
        int messageSize = frameLength - MessageCodec.REQUEST_HEADER_LENGTH;

        boolean needRelease = false;
        try {
            ApiMethod api = apiManager.getApi(apiKey);
            if (api == null) {
                LOG.warn("Received unknown API key {}.", apiKey);
                needRelease = true;
                return;
            }

            ApiMessage requestMessage = api.getRequestConstructor().get();
            requestMessage.parseFrom(buffer, messageSize);
            // Most request types are parsed entirely into objects at this point. For those we can
            // release the underlying buffer.
            // However, some (like produce) retain a reference to the buffer. For those requests we
            // cannot release the buffer early, but only when request processing is done.
            if (!requestMessage.isLazilyParsed()) {
                needRelease = true;
            }

            if (state.isAuthenticating() && apiKey != ApiKeys.API_VERSIONS.id) {
                handleAuthenticateRequest(apiKey, requestId, requestMessage, ctx);
                return;
            }

            FlussRequest request =
                    new FlussRequest(
                            apiKey,
                            apiVersion,
                            requestId,
                            api,
                            requestMessage,
                            buffer,
                            listenerName,
                            ctx);
            // TODO: we can introduce a smarter and dynamic strategy to distribute requests to
            //  channels
            int channelIndex =
                    MathUtils.murmurHash(ctx.channel().id().asLongText().hashCode()) % numChannels;
            requestChannels[channelIndex].putRequest(request);
        } catch (Throwable t) {
            needRelease = true;
            LOG.error("Error while parsing request.", t);
            ApiError error = ApiError.fromThrowable(t);
            ctx.writeAndFlush(encodeErrorResponse(ctx.alloc(), requestId, error));
        } finally {
            if (needRelease) {
                buffer.release();
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        // TODO: connection metrics (count, client tags, receive request avg idle time, etc.)
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state().equals(IdleState.ALL_IDLE)) {
                LOG.warn("Connection {} is idle, closing...", ctx.channel().remoteAddress());
                ctx.close();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // debug level to avoid too many logs if NLB(Network Load Balancer is mounted, see
        // more detail in #377
        // may revert to warn level if we found warn level is necessary
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Connection [{}] got exception in Netty server pipeline: \n{}",
                    ctx.channel().remoteAddress(),
                    ExceptionUtils.stringifyException(cause));
        }

        switchState(ConnectionState.FAIL);
        ByteBuf byteBuf = encodeServerFailure(ctx.alloc(), ApiError.fromThrowable(cause));
        ctx.writeAndFlush(byteBuf).addListener(ChannelFutureListener.CLOSE);
    }

    private void handleAuthenticateRequest(
            short apiKey, int requestId, ApiMessage requestMessage, ChannelHandlerContext ctx) {
        if (apiKey != ApiKeys.AUTHENTICATE.id) {
            LOG.warn(
                    "Connection is still in the authentication process. Unable to handle API key: {}.",
                    apiKey);
            throw new AuthenticationException(
                    "The connection has not completed authentication yet.");
        }

        AuthenticateRequest authenticateRequest = (AuthenticateRequest) requestMessage;
        if (!authenticator.protocol().equals(authenticateRequest.getProtocol())) {
            throw new AuthenticationException(
                    String.format(
                            "Authenticate protocol not match: protocol of server is %s while protocol of client is %s",
                            authenticator.protocol(), authenticateRequest.getProtocol()));
        }

        AuthenticateResponse authenticateResponse = new AuthenticateResponse();
        if (!authenticator.isComplete()) {
            byte[] token = authenticateRequest.getToken();
            byte[] response =
                    !authenticator.isComplete()
                            ? authenticator.evaluateResponse(token)
                            : new byte[0];
            authenticateResponse.setChallenge(response);
        }
        sendAuthenticateResponse(requestId, ctx, authenticateResponse);

        if (authenticator.isComplete()) {
            switchState(ConnectionState.READY);
        }
    }

    private void sendAuthenticateResponse(
            int requestId, ChannelHandlerContext ctx, AuthenticateResponse authenticateResponse) {
        ByteBufAllocator alloc = ctx.alloc();
        try {
            Send send = encodeSuccessResponse(alloc, requestId, authenticateResponse);
            send.writeTo(ctx);
            ctx.flush();
        } catch (Throwable t) {
            LOG.error("Failed to send response to client.", t);
            ApiError error = ApiError.fromThrowable(t);
            // TODO: use a memory managed allocator
            alloc = ctx.alloc();
            ByteBuf byteBuf = encodeErrorResponse(alloc, requestId, error);
            ctx.writeAndFlush(byteBuf);
        }
    }

    private void switchState(ConnectionState targetState) {
        LOG.info("switch state form {} to {}", state, targetState);
        state = targetState;
    }

    private enum ConnectionState {
        AUTHENTICATING,
        READY,
        FAIL;

        public boolean isReady() {
            return this == READY;
        }

        public boolean isAuthenticating() {
            return this == AUTHENTICATING;
        }
    }
}
