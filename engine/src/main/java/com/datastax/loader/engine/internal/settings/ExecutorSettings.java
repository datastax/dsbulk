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
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.loader.commons.config.LoaderConfig;
import com.datastax.loader.engine.WorkflowType;
import com.datastax.loader.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.loader.executor.api.ContinuousRxJavaBulkExecutor;
import com.datastax.loader.executor.api.ContinuousRxJavaBulkExecutorBuilder;
import com.datastax.loader.executor.api.DefaultRxJavaBulkExecutor;
import com.datastax.loader.executor.api.DefaultRxJavaBulkExecutorBuilder;
import com.datastax.loader.executor.api.RxJavaBulkExecutor;
import com.datastax.loader.executor.api.listener.ExecutionListener;
import com.datastax.loader.executor.api.listener.MetricsCollectingExecutionListener;
import com.datastax.loader.executor.api.reader.RxJavaBulkReader;
import com.datastax.loader.executor.api.writer.RxJavaBulkWriter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class ExecutorSettings {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorSettings.class);

  private final LoaderConfig config;

  ExecutorSettings(LoaderConfig config) {
    this.config = config;
  }

  public RxJavaBulkWriter newWriteExecutor(Session session, ExecutionListener executionListener) {
    return newBulkExecutor(session, executionListener, WorkflowType.WRITE);
  }

  public RxJavaBulkReader newReadExecutor(
      Session session, MetricsCollectingExecutionListener executionListener) {
    return newBulkExecutor(session, executionListener, WorkflowType.READ);
  }

  public int getMaxConcurrentReads() {
    return config.getInt("maxConcurrentReads");
  }

  private RxJavaBulkExecutor newBulkExecutor(
      Session session, ExecutionListener executionListener, WorkflowType workflowType) {
    if (workflowType == WorkflowType.READ) {
      if (continuousPagingAvailable(session)) {
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
        LOGGER.warn("Continuous paging is not available, read performance will not be optimal");
      }
    }
    DefaultRxJavaBulkExecutorBuilder builder = DefaultRxJavaBulkExecutor.builder(session);
    configure(builder, executionListener);
    return builder.build();
  }

  private boolean continuousPagingAvailable(Session session) {
    return session instanceof ContinuousPagingSession
        && session
                .getCluster()
                .getConfiguration()
                .getProtocolOptions()
                .getProtocolVersion()
                .compareTo(ProtocolVersion.DSE_V1)
            >= 0;
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
