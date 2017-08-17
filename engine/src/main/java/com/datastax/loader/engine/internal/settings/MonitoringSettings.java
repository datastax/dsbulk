/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import com.datastax.loader.commons.config.LoaderConfig;
import com.datastax.loader.engine.internal.metrics.MetricsManager;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/** */
public class MonitoringSettings {

  private final LoaderConfig config;
  private final String operationId;

  MonitoringSettings(LoaderConfig config, String operationId) {
    this.config = config;
    this.operationId = operationId;
  }

  public MetricsManager newMetricsManager() {
    ThreadFactory threadFactory =
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("reporter-%d")
            .setPriority(Thread.MIN_PRIORITY)
            .build();
    ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, threadFactory);
    TimeUnit rateUnit = config.getEnum(TimeUnit.class, "rateUnit");
    TimeUnit durationUnit = config.getEnum(TimeUnit.class, "durationUnit");
    Duration reportInterval = config.getDuration("reportInterval");
    long expectedWrites = config.getLong("expectedWrites");
    long expectedReads = config.getLong("expectedReads");
    boolean jmx = config.getBoolean("jmx");
    return new MetricsManager(
        operationId,
        scheduler,
        rateUnit,
        durationUnit,
        expectedWrites,
        expectedReads,
        jmx,
        reportInterval);
  }
}
