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

package com.alibaba.fluss.fs.gs;

import com.alibaba.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpObject;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.QueryStringDecoder;
import com.alibaba.fluss.shaded.netty4.io.netty.util.AsciiString;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.shaded.guava32.com.google.common.net.HttpHeaders.CONTENT_ENCODING;
import static com.alibaba.fluss.shaded.guava32.com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;

public class GSPathServerHandler extends SimpleChannelInboundHandler<HttpObject> {

    private static volatile Boolean created = false;

    private final String bucket;
    private final String path;
    private final String key;

    public GSPathServerHandler(String bucket, String path, String key) {
        this.bucket = bucket;
        this.path = path;
        this.key = key;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        if (msg instanceof HttpRequest) {
            HttpRequest req = (HttpRequest) msg;

            try {
                URI url = URI.create(req.uri());
                if (req.method().equals(HttpMethod.POST)) {
                    postRequest(ctx, url, req);
                } else if (req.method().equals(HttpMethod.DELETE)) {
                    deleteRequest(ctx, req);
                } else {
                    getRequest(ctx, url, req);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void deleteRequest(ChannelHandlerContext ctx, HttpRequest req) throws IOException {
        created = false;
        jsonResponse(ctx, req, path + "/delete-object.json", OK);
    }

    private void postRequest(ChannelHandlerContext ctx, URI url, HttpRequest req)
            throws IOException {
        if (url.getPath().endsWith("/token")) {
            jsonResponse(ctx, req, "create-token.json");
        } else if (url.getPath().contains(path + "/")) {
            jsonResponse(ctx, req, "fluss/get-directory-object.json", OK);
        } else {
            Map<String, List<String>> params = new QueryStringDecoder(url.toString()).parameters();
            if (url.toString().contains("multipart")) {
                jsonResponse(ctx, req, path + "/get-directory-object.json", OK);
            } else {
                created = true;
                jsonResponse(ctx, req, path + "/get-object.json", OK);
            }
        }
    }

    private void getRequest(ChannelHandlerContext ctx, URI url, HttpRequest req)
            throws IOException {
        if (url.getPath().endsWith("/" + bucket + "/o")) {
            if (created) {
                jsonResponse(ctx, req, path + "/list-objects.json", OK);
            } else {
                jsonResponse(ctx, req, path + "/list-objects-empty.json", NOT_FOUND);
            }
        } else if (url.getPath().endsWith("/" + bucket + "/o/" + path + "/")) {
            jsonResponse(ctx, req, path + "/list-objects-empty.json", NOT_FOUND);
        } else if (url.getPath()
                .endsWith("/download/storage/v1/b/" + bucket + "/o/" + path + "/" + key)) {
            response(ctx, req, new byte[] {1, 2, 3, 4, 5}, OK, APPLICATION_JSON);
        } else {
            if (!created) {
                jsonResponse(ctx, req, path + "/get-object-not-found.json", NOT_FOUND);
            } else {
                jsonResponse(ctx, req, path + "/get-object.json");
            }
        }
    }

    private void jsonResponse(ChannelHandlerContext ctx, HttpRequest req, String path)
            throws IOException {
        jsonResponse(ctx, req, path, OK);
    }

    private void jsonResponse(
            ChannelHandlerContext ctx, HttpRequest req, String path, HttpResponseStatus ok)
            throws IOException {
        response(ctx, req, readFromResources(path), ok, APPLICATION_JSON);
    }

    private static void response(
            ChannelHandlerContext ctx,
            HttpRequest req,
            byte[] bytes,
            HttpResponseStatus status,
            AsciiString contentType) {
        FullHttpResponse response =
                new DefaultFullHttpResponse(
                        req.protocolVersion(), status, Unpooled.wrappedBuffer(bytes));
        response.headers()
                .set(CONTENT_TYPE, contentType)
                .set("Location", "http://localhost:8080/resumbable-upload")
                .setInt(CONTENT_LENGTH, response.content().readableBytes());

        response.headers().remove(CONTENT_ENCODING);
        response.headers().set(CONNECTION, CLOSE);
        ctx.write(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }

    private byte[] readFromResources(String path) throws IOException {
        return GSPathServerHandler.class.getClassLoader().getResourceAsStream(path).readAllBytes();
    }
}
