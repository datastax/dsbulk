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
import com.datastax.loader.executor.api.ContinuousReactorBulkExecutor;
import com.datastax.loader.executor.api.ContinuousReactorBulkExecutorBuilder;
import com.datastax.loader.executor.api.DefaultReactorBulkExecutor;
import com.datastax.loader.executor.api.DefaultReactorBulkExecutorBuilder;
import com.datastax.loader.executor.api.ReactiveBulkExecutor;
import com.datastax.loader.executor.api.ReactorBulkExecutor;
import com.datastax.loader.executor.api.listener.ExecutionListener;
import com.datastax.loader.executor.api.listener.MetricsCollectingExecutionListener;
import com.datastax.loader.executor.api.reader.ReactorBulkReader;
import com.datastax.loader.executor.api.writer.ReactiveBulkWriter;
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
  private ThreadPoolExecutor executor;

  ExecutorSettings(LoaderConfig config) {
    this.config = config;
  }

  public ReactiveBulkWriter newWriteExecutor(Session session, ExecutionListener executionListener) {
    return newBulkExecutor(session, executionListener, WorkflowType.WRITE);
  }

  public ReactorBulkReader newReadExecutor(
      Session session, MetricsCollectingExecutionListener executionListener) {
    return newBulkExecutor(session, executionListener, WorkflowType.READ);
  }

  public ThreadPoolExecutor getExecutorThreadPool() {
    return executor;
  }

  private ReactorBulkExecutor newBulkExecutor(
      Session session, ExecutionListener executionListener, WorkflowType workflowType) {
    if (workflowType == WorkflowType.READ) {
      if (continuousPagingAvailable(session)) {
        ContinuousReactorBulkExecutorBuilder builder =
            ContinuousReactorBulkExecutor.builder(((ContinuousPagingSession) session));
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
    DefaultReactorBulkExecutorBuilder builder = DefaultReactorBulkExecutor.builder(session);
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
      AbstractBulkExecutorBuilder<? extends ReactiveBulkExecutor> builder,
      ExecutionListener executionListener) {
    int threads = config.getThreads("maxThreads");
    // will be closed when the Bulk Executor gets closed
    executor =
        new ThreadPoolExecutor(
            0,
            threads,
            60,
            SECONDS,
            new SynchronousQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("bulk-executor-%0,2d").build(),
            new ThreadPoolExecutor.CallerRunsPolicy());
    builder
        .withExecutor(executor)
        .withExecutionListener(executionListener)
        .withMaxInFlightRequests(config.getInt("maxInflight"))
        .withMaxRequestsPerSecond(config.getInt("maxPerSecond"))
        .failSafe();
  }
}
