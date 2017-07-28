/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.Session;
import com.datastax.loader.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.loader.executor.api.ContinuousRxJavaBulkExecutor;
import com.datastax.loader.executor.api.ContinuousRxJavaBulkExecutorBuilder;
import com.datastax.loader.executor.api.DefaultRxJavaBulkExecutor;
import com.datastax.loader.executor.api.DefaultRxJavaBulkExecutorBuilder;
import com.datastax.loader.executor.api.RxJavaBulkExecutor;
import com.datastax.loader.executor.api.writer.ReactiveBulkWriter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

/** */
public class ExecutorSettings {

  private final Config config;

  public ExecutorSettings(Config config) {
    this.config = config;
  }

  public ReactiveBulkWriter newWriteEngine(Session session) {
    if (session instanceof ContinuousPagingSession) {
      ContinuousRxJavaBulkExecutorBuilder builder =
          ContinuousRxJavaBulkExecutor.builder(((ContinuousPagingSession) session));
      configure(builder);
      Config continuousPagingConfig = config.getConfig("continuous-paging");
      ContinuousPagingOptions options =
          ContinuousPagingOptions.builder()
              .withPageSize(
                  continuousPagingConfig.getInt("page-size"),
                  continuousPagingConfig.getEnum(
                      ContinuousPagingOptions.PageUnit.class, "page-unit"))
              .withMaxPages(continuousPagingConfig.getInt("max-pages"))
              .withMaxPagesPerSecond(continuousPagingConfig.getInt("max-pages-per-second"))
              .build();
      builder.withContinuousPagingOptions(options);
      return builder.build();
    } else {
      DefaultRxJavaBulkExecutorBuilder builder = DefaultRxJavaBulkExecutor.builder(session);
      configure(builder);
      return builder.build();
    }
  }

  private void configure(AbstractBulkExecutorBuilder<? extends RxJavaBulkExecutor> builder) {
    String maxThreads = config.getString("max-threads");
    int threads = SettingsUtils.parseNumThreads(maxThreads);
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            0,
            threads,
            60,
            SECONDS,
            new SynchronousQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("bulk-executor-%d").build(),
            new ThreadPoolExecutor.CallerRunsPolicy());
    builder
        .withExecutor(executor)
        .withMaxInFlightRequests(config.getInt("max-inflight"))
        .withMaxRequestsPerSecond(config.getInt("max-per-second"))
        .failSafe();
  }
}
