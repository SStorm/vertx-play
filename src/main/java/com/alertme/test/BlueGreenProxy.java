/**
 * Copyright (C) 2014 AlertMe.com Ltd
 */
package com.alertme.test;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.vertx.core.buffer.Buffer.buffer;
import static java.lang.String.format;

public class BlueGreenProxy extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(BlueGreenProxy.class);

    private static AtomicBoolean statisticianCreated = new AtomicBoolean(false);

    private final URI primaryHost;

    private final URI secondaryHost;

    private int port;

    {
        primaryHost = URI.create(System.getProperty("primary.host"));
        secondaryHost = URI.create(System.getProperty("secondary.host"));
        port = Integer.valueOf(System.getProperty("port", "8090"));
    }

    public void start() throws Exception {
        if (statisticianCreated.compareAndSet(false, true)) {
            getVertx().deployVerticle("com.alertme.test.Statistician", res -> {
                if (res.failed()) {
                    logger.error(format("Failed to create statistician: %s", res.result()));
                }
            });
        }
        logger.info(format("Initializing HTTP Server Proxy instance %s", Thread.currentThread().getName()));
        logger.info(format("Primary host: %s, secondary host: %s", primaryHost, secondaryHost));
        final HttpClient client = vertx.createHttpClient();
        vertx.createHttpServer().requestHandler(request -> {
            collectStats(request);

            final Buffer requestBody = buffer();
            request.handler(requestBody::appendBuffer);

            request.endHandler(end -> {
                final ProxyRequest primaryProxyRequest = new ProxyRequest(request.method(), request.headers(),
                        request.uri(), requestBody, client, primaryHost);
                final ProxyRequest secondaryProxyRequest = new ProxyRequest(request.method(), request.headers(),
                        request.uri(), requestBody, client, secondaryHost);

                final TriConsumer<Buffer, Integer, MultiMap> writeResponse = (body, code, headers) -> {
                    if (headers != null) {
                        request.response().headers().setAll(headers);
                    }
                    if (code == null) {
                        logger.error("Code is empty, assuming server error occurred");
                        code = INTERNAL_SERVER_ERROR.code();
                    }
                    request.response().setStatusCode(code);
                    request.response().setChunked(true);
                    if (body != null) {
                        request.response().write(body);
                    }
                    request.response().end();
                    // TODO if we start writing async, then we should call this only when request ended
                };

                if (request.method() == HttpMethod.GET || request.method() == HttpMethod.HEAD
                        || request.method() == HttpMethod.OPTIONS) {
                    primaryProxyRequest.onFailure((body, code, headers) -> secondaryProxyRequest.request());
                    primaryProxyRequest.onSuccess(writeResponse);
                    secondaryProxyRequest.onFailure((body, code, headers) -> primaryProxyRequest.writeResponse(writeResponse));
                    secondaryProxyRequest.onSuccess(writeResponse);
                    primaryProxyRequest.request();
                } else {
                    primaryProxyRequest.onComplete(writeResponse);
                    primaryProxyRequest.request();
                    secondaryProxyRequest.request();
                }
            });
        }).listen(port);
    }

    @FunctionalInterface
    interface TriConsumer<A, B, C> {
        void apply(A a, B b, C c);
    }

    private static class ProxyRequest {

        private final HttpClient client;

        private final URI proxyToUri;
        private final HttpMethod method;
        private final String uri;
        private final Buffer requestBody;

        private TriConsumer<Buffer, Integer, MultiMap> onFailure;
        private TriConsumer<Buffer, Integer, MultiMap> onSuccess;
        private TriConsumer<Buffer, Integer, MultiMap> onComplete;

        private Integer code;
        private MultiMap headers;
        private Buffer body;

        public ProxyRequest(final HttpMethod method, final MultiMap headers, final String uri, final Buffer requestBody,
                            final HttpClient client, final URI proxyToUri) {

            this.client = client;
            this.proxyToUri = proxyToUri;
            this.body = buffer();
            this.method = method;
            this.headers = headers;
            this.uri = uri;
            this.requestBody = requestBody;
        }

        public void request() {
            final HttpClientRequest httpRequest = client.request(method, proxyToUri.getPort(), proxyToUri.getHost(), uri, resp -> {
                headers = resp.headers();
                code = resp.statusCode();
                resp.handler(this.body::appendBuffer);
                resp.endHandler(end -> {
                    if (code >= 200 && code < 300) {
                        call(onSuccess);
                    } else {
                        call(onFailure);
                    }
                    call(onComplete);
                });
                // TODO can we start writing without waiting the whole buffer?
            });
            httpRequest.exceptionHandler(ex -> {
                logger.error(format("Got exception processing request: %s", ex.getMessage()));
                code = INTERNAL_SERVER_ERROR.code();
                call(onFailure);
                call(onComplete);
            });
            httpRequest.headers().setAll(headers);
            httpRequest.write(requestBody);
            httpRequest.end();
        }

        private void call(final TriConsumer<Buffer, Integer, MultiMap> function) {
            if (function != null) {
                function.apply(body, code, headers);
            }
        }

        public void onFailure(final TriConsumer<Buffer, Integer, MultiMap> onFail) {
            this.onFailure = onFail;
        }

        public void onSuccess(final TriConsumer<Buffer, Integer, MultiMap> onSuccess) {
            this.onSuccess = onSuccess;
        }

        public void onComplete(final TriConsumer<Buffer, Integer, MultiMap> onComplete) {
            this.onComplete = onComplete;
        }

        public void writeResponse(final TriConsumer<Buffer, Integer, MultiMap> responseHandler) {
            call(responseHandler);
        }
    }

    private void collectStats(final HttpServerRequest request) {
        getVertx().eventBus().publish("proxy.stats", 1);
    }
}
