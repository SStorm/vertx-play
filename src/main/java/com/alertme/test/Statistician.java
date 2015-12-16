/**
 * Copyright (C) 2014 AlertMe.com Ltd
 */
package com.alertme.test;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import static java.lang.String.format;

public class Statistician extends AbstractVerticle {

    private Logger logger = LoggerFactory.getLogger(Statistician.class);

    private static long lastStatsOutput = 0;
    private static int requestCount = 0;

    public void start() {
        logger.info("Starting statistician");
        getVertx().eventBus().consumer("proxy.stats", handler -> {
            if (handler.body() instanceof Integer) {
                requestCount++;
                long currentTime = System.currentTimeMillis();
                if (currentTime - 10000 > lastStatsOutput) {
                    double rps = requestCount / 10;
                    logger.info(format("Requests processed: %s, %s rps", requestCount, rps));
                    lastStatsOutput = currentTime;
                    requestCount = 0;
                }
            }
        });
    }
}
