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
import com.datastax.loader.commons.config.LoaderConfig;
import com.datastax.loader.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.loader.executor.api.ContinuousRxJavaBulkExecutor;
import com.datastax.loader.executor.api.ContinuousRxJavaBulkExecutorBuilder;
import com.datastax.loader.executor.api.DefaultRxJavaBulkExecutor;
import com.datastax.loader.executor.api.DefaultRxJavaBulkExecutorBuilder;
import com.datastax.loader.executor.api.RxJavaBulkExecutor;
import com.datastax.loader.executor.api.listener.ExecutionListener;
import com.datastax.loader.executor.api.writer.ReactiveBulkWriter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

/** */
public class ExecutorSettings {

  private final LoaderConfig config;

  ExecutorSettings(LoaderConfig config) {
    this.config = config;
  }

  public ReactiveBulkWriter newWriteExecutor(Session session, ExecutionListener executionListener) {
    if (session instanceof ContinuousPagingSession) {
      ContinuousRxJavaBulkExecutorBuilder builder =
          ContinuousRxJavaBulkExecutor.builder(((ContinuousPagingSession) session));
      configure(builder, executionListener);
      Config continuousPagingConfig = config.getConfig("continuousPaging");
      ContinuousPagingOptions options =
          ContinuousPagingOptions.builder()
              .withPageSize(
                  continuousPagingConfig.getInt("pageSize"),
                  continuousPagingConfig.getEnum(
                      ContinuousPagingOptions.PageUnit.class, "pageUnit"))
              .withMaxPages(continuousPagingConfig.getInt("maxPages"))
              .withMaxPagesPerSecond(continuousPagingConfig.getInt("maxPagesPerSecond"))
              .build();
      builder.withContinuousPagingOptions(options);
      return builder.build();
    } else {
      DefaultRxJavaBulkExecutorBuilder builder = DefaultRxJavaBulkExecutor.builder(session);
      configure(builder, executionListener);
      return builder.build();
    }
  }

  private void configure(
      AbstractBulkExecutorBuilder<? extends RxJavaBulkExecutor> builder,
      ExecutionListener executionListener) {
    int threads = config.getThreads("maxThreads");
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
        .withExecutionListener(executionListener)
        .withMaxInFlightRequests(config.getInt("maxInflight"))
        .withMaxRequestsPerSecond(config.getInt("maxPerSecond"))
        .failSafe();
  }
}
