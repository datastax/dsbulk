/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.config.ConfigUtils;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.dsbulk.executor.api.ContinuousReactorBulkExecutor;
import com.datastax.dsbulk.executor.api.ContinuousReactorBulkExecutorBuilder;
import com.datastax.dsbulk.executor.api.DefaultReactorBulkExecutor;
import com.datastax.dsbulk.executor.api.DefaultReactorBulkExecutorBuilder;
import com.datastax.dsbulk.executor.api.ReactiveBulkExecutor;
import com.datastax.dsbulk.executor.api.ReactorBulkExecutor;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.listener.MetricsCollectingExecutionListener;
import com.datastax.dsbulk.executor.api.reader.ReactorBulkReader;
import com.datastax.dsbulk.executor.api.writer.ReactiveBulkWriter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class ExecutorSettings implements SettingsValidator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorSettings.class);

  private final LoaderConfig config;
  private ThreadPoolExecutor executor;

  ExecutorSettings(LoaderConfig config) {
    this.config = config;
  }

  public ReactiveBulkWriter newWriteExecutor(Session session, ExecutionListener executionListener) {
    return newBulkExecutor(session, executionListener, WorkflowType.LOAD);
  }

  public ReactorBulkReader newReadExecutor(
      Session session, MetricsCollectingExecutionListener executionListener) {
    return newBulkExecutor(session, executionListener, WorkflowType.UNLOAD);
  }

  @SuppressWarnings("unused")
  public ThreadPoolExecutor getExecutorThreadPool() {
    return executor;
  }

  private ReactorBulkExecutor newBulkExecutor(
      Session session, ExecutionListener executionListener, WorkflowType workflowType) {
    if (workflowType == WorkflowType.UNLOAD) {
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

  public void validateConfig(WorkflowType type) throws IllegalArgumentException {
    try {
      config.getThreads("maxThreads");
      config.getInt("maxPerSecond");
      config.getInt("maxInFlight");
    } catch (ConfigException e) {
      ConfigUtils.badConfigToIllegalArgument(e, "executor");
    }
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
        .withMaxInFlightRequests(config.getInt("maxInFlight"))
        .withMaxRequestsPerSecond(config.getInt("maxPerSecond"))
        .failSafe();
  }
}
