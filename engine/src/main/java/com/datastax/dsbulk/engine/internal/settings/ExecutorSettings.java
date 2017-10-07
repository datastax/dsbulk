/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
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
import com.datastax.dsbulk.executor.api.writer.ReactorBulkWriter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class ExecutorSettings implements SettingsValidator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorSettings.class);

  private final LoaderConfig config;

  ExecutorSettings(LoaderConfig config) {
    this.config = config;
  }

  public ReactorBulkWriter newWriteExecutor(Session session, ExecutionListener executionListener) {
    return newBulkExecutor(session, executionListener, WorkflowType.LOAD);
  }

  public ReactorBulkReader newReadExecutor(
      Session session, MetricsCollectingExecutionListener executionListener) {
    return newBulkExecutor(session, executionListener, WorkflowType.UNLOAD);
  }

  private ReactorBulkExecutor newBulkExecutor(
      Session session, ExecutionListener executionListener, WorkflowType workflowType) {
    if (workflowType == WorkflowType.UNLOAD) {
      Config continuousPagingConfig = config.getConfig("continuousPaging");
      if (continuousPagingConfig.getBoolean("enabled")) {
        if (continuousPagingAvailable(session)) {
          continuousPagingAvailable(session);
          ContinuousReactorBulkExecutorBuilder builder =
              ContinuousReactorBulkExecutor.builder(((ContinuousPagingSession) session));
          configure(builder, executionListener);
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
    }
    DefaultReactorBulkExecutorBuilder builder = DefaultReactorBulkExecutor.builder(session);
    configure(builder, executionListener);
    return builder.build();
  }

  public void validateConfig(WorkflowType type) throws BulkConfigurationException {
    try {
      config.getInt("maxPerSecond");
      config.getInt("maxInFlight");
      Config continuousPagingConfig = config.getConfig("continuousPaging");
      if (continuousPagingConfig.getBoolean("enabled")) {
        continuousPagingConfig.getInt("pageSize");
        continuousPagingConfig.getEnum(ContinuousPagingOptions.PageUnit.class, "pageUnit");
        continuousPagingConfig.getInt("maxPages");
        continuousPagingConfig.getInt("maxPagesPerSecond");
      }
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "executor");
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
    // will be closed when the Bulk Executor gets closed
    ExecutorService executor = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat("bulk-executor-%0,2d").build());
    builder
        .withExecutor(executor)
        .withExecutionListener(executionListener)
        .withMaxInFlightRequests(config.getInt("maxInFlight"))
        .withMaxRequestsPerSecond(config.getInt("maxPerSecond"))
        .failSafe();
  }
}
