/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.dsbulk.executor.api.ReactiveBulkExecutor;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.listener.MetricsCollectingExecutionListener;
import com.datastax.dsbulk.executor.reactor.ContinuousReactorBulkExecutor;
import com.datastax.dsbulk.executor.reactor.ContinuousReactorBulkExecutorBuilder;
import com.datastax.dsbulk.executor.reactor.DefaultReactorBulkExecutor;
import com.datastax.dsbulk.executor.reactor.DefaultReactorBulkExecutorBuilder;
import com.datastax.dsbulk.executor.reactor.ReactorBulkExecutor;
import com.datastax.dsbulk.executor.reactor.reader.ReactorBulkReader;
import com.datastax.dsbulk.executor.reactor.writer.ReactorBulkWriter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class ExecutorSettings {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorSettings.class);

  private static final String MAX_PER_SECOND = "maxPerSecond";
  private static final String MAX_IN_FLIGHT = "maxInFlight";
  private static final String CONTINUOUS_PAGING = "continuousPaging";
  private static final String ENABLED = "enabled";
  private static final String PAGE_SIZE = "pageSize";
  private static final String PAGE_UNIT = "pageUnit";
  private static final String MAX_PAGES = "maxPages";
  private static final String MAX_PAGES_PER_SECOND = "maxPagesPerSecond";

  private final LoaderConfig config;

  private int maxPerSecond;
  private int maxInFlight;
  private boolean continuousPagingEnabled;
  private int pageSize;
  private int maxPages;
  private int maxPagesPerSecond;
  private ContinuousPagingOptions.PageUnit pageUnit;

  ExecutorSettings(LoaderConfig config) {
    this.config = config;
  }

  public void init() {
    try {
      maxPerSecond = config.getInt(MAX_PER_SECOND);
      maxInFlight = config.getInt(MAX_IN_FLIGHT);
      Config continuousPagingConfig = config.getConfig(CONTINUOUS_PAGING);
      continuousPagingEnabled = continuousPagingConfig.getBoolean(ENABLED);
      if (continuousPagingEnabled) {
        pageSize = continuousPagingConfig.getInt(PAGE_SIZE);
        pageUnit =
            continuousPagingConfig.getEnum(ContinuousPagingOptions.PageUnit.class, PAGE_UNIT);
        maxPages = continuousPagingConfig.getInt(MAX_PAGES);
        maxPagesPerSecond = continuousPagingConfig.getInt(MAX_PAGES_PER_SECOND);
      }
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "executor");
    }
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
      if (continuousPagingEnabled) {
        if (continuousPagingAvailable(session)) {
          continuousPagingAvailable(session);
          ContinuousReactorBulkExecutorBuilder builder =
              ContinuousReactorBulkExecutor.builder(((ContinuousPagingSession) session));
          configure(builder, executionListener);
          ContinuousPagingOptions options =
              ContinuousPagingOptions.builder()
                  .withPageSize(pageSize, pageUnit)
                  .withMaxPages(maxPages)
                  .withMaxPagesPerSecond(maxPagesPerSecond)
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

  public int getMaxInFlight() {
    return maxInFlight;
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
    builder
        .withExecutionListener(executionListener)
        .withMaxInFlightRequests(maxInFlight)
        .withMaxRequestsPerSecond(maxPerSecond)
        .failSafe();
  }
}
