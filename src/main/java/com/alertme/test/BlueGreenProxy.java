/**
 * Copyright (C) 2014 AlertMe.com Ltd
 */
package com.alertme.test;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class BlueGreenProxy extends AbstractVerticle {

    private Logger logger = LoggerFactory.getLogger(BlueGreenProxy.class);

    private static AtomicBoolean statisticianCreated = new AtomicBoolean(false);

    String targetHost = "statecache.romanas2.dev2.bgch.io";
    {
        targetHost = System.getProperty("HOST");
    }

    public void start() throws Exception {
        if (statisticianCreated.compareAndSet(false, true)) {
            getVertx().deployVerticle("com.alertme.test.Statistician", res -> {
                if (res.failed()) {
                    logger.error("Failed to create statistician: " + res.result());
                }
            });
        }
        logger.info("Creating HTTP Server");
        logger.info("Proxying to: " + targetHost);
        HttpClient client = vertx.createHttpClient();
        vertx.createHttpServer().requestHandler(req -> {
            collectStats(req);
            HttpClientRequest httpRequest = client.request(req.method(), 8090, targetHost, req.uri(), resp -> {
                req.response().setChunked(true);
                req.response().headers().setAll(resp.headers());
                req.response().setStatusCode(resp.statusCode());
                resp.handler(body -> {
                    req.response().write(body);
                });
                resp.endHandler(end -> {
                    req.response().end();
                });
            });
            httpRequest.exceptionHandler(ex -> {
                logger.error("Got exception processing request: " + ex.getMessage());
                req.response().setStatusCode(500);
                req.response().end();
            });
            httpRequest.headers().setAll(req.headers());
            req.handler(body -> {
                httpRequest.write(body);
            });
            req.endHandler(end -> {
                httpRequest.end();
            });

        }).listen(6666);
    }

    private void collectStats(HttpServerRequest request) {
        getVertx().eventBus().publish("proxy.stats", 1);
    }
}
