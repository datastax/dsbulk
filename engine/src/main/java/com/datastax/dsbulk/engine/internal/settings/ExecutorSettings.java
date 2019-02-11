/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
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
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final String MAX_CONCURRENT_REQUESTS = "maxConcurrentQueries";

  private final LoaderConfig config;

  private int maxPerSecond;
  private int maxInFlight;
  private boolean continuousPagingEnabled;
  private int pageSize;
  private int maxPages;
  private int maxPagesPerSecond;
  private ContinuousPagingOptions.PageUnit pageUnit;
  private int maxConcurrentQueries;

  ExecutorSettings(LoaderConfig config) {
    this.config = config;
  }

  public void init() {
    try {
      maxPerSecond = config.getInt(MAX_PER_SECOND);
      maxInFlight = config.getInt(MAX_IN_FLIGHT);
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "executor");
    }
    try {
      Config continuousPagingConfig = config.getConfig(CONTINUOUS_PAGING);
      continuousPagingEnabled = continuousPagingConfig.getBoolean(ENABLED);
      if (continuousPagingEnabled) {
        pageSize = continuousPagingConfig.getInt(PAGE_SIZE);
        pageUnit =
            continuousPagingConfig.getEnum(ContinuousPagingOptions.PageUnit.class, PAGE_UNIT);
        maxPages = continuousPagingConfig.getInt(MAX_PAGES);
        maxPagesPerSecond = continuousPagingConfig.getInt(MAX_PAGES_PER_SECOND);
        maxConcurrentQueries = continuousPagingConfig.getInt(MAX_CONCURRENT_REQUESTS);
      }
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "executor.continuousPaging");
    }
  }

  public Optional<Integer> getMaxInFlight() {
    return maxInFlight > 0 ? Optional.of(maxInFlight) : Optional.empty();
  }

  public Optional<Integer> getMaxConcurrentQueries() {
    return maxConcurrentQueries > 0 ? Optional.of(maxConcurrentQueries) : Optional.empty();
  }

  public ReactorBulkWriter newWriteExecutor(Session session, ExecutionListener executionListener) {
    return newBulkExecutor(session, executionListener, false, false);
  }

  public ReactorBulkReader newReadExecutor(
      Session session, MetricsCollectingExecutionListener executionListener, boolean searchQuery) {
    return newBulkExecutor(session, executionListener, true, searchQuery);
  }

  private ReactorBulkExecutor newBulkExecutor(
      Session session, ExecutionListener executionListener, boolean read, boolean searchQuery) {
    if (read) {
      if (continuousPagingEnabled) {
        if (searchQuery) {
          LOGGER.warn(
              "Continuous paging is enabled but is not compatible with search queries; disabling.");
          return newDefaultExecutor(session, executionListener);
        }
        if (continuousPagingAvailable(session)) {
          return newContinuousExecutor((ContinuousPagingSession) session, executionListener);
        } else {
          LOGGER.warn(
              "Continuous paging is not available, read performance will not be optimal. "
                  + "Check your remote DSE cluster configuration, and ensure that "
                  + "the configured consistency level is either ONE or LOCAL_ONE.");
        }
      } else {
        LOGGER.debug("Continuous paging was disabled by configuration.");
      }
    }
    return newDefaultExecutor(session, executionListener);
  }

  private ReactorBulkExecutor newDefaultExecutor(
      Session session, ExecutionListener executionListener) {
    DefaultReactorBulkExecutorBuilder builder = DefaultReactorBulkExecutor.builder(session);
    configureExecutor(builder, executionListener);
    return builder.build();
  }

  private ReactorBulkExecutor newContinuousExecutor(
      ContinuousPagingSession session, ExecutionListener executionListener) {
    ContinuousReactorBulkExecutorBuilder builder = ContinuousReactorBulkExecutor.builder(session);
    configureExecutor(builder, executionListener);
    ContinuousPagingOptions options =
        ContinuousPagingOptions.builder()
            .withPageSize(pageSize, pageUnit)
            .withMaxPages(maxPages)
            .withMaxPagesPerSecond(maxPagesPerSecond)
            .build();
    return builder
        .withContinuousPagingOptions(options)
        .withMaxInFlightQueries(maxConcurrentQueries)
        .build();
  }

  private boolean continuousPagingAvailable(Session session) {
    Configuration configuration = session.getCluster().getConfiguration();
    ProtocolVersion protocolVersion = configuration.getProtocolOptions().getProtocolVersion();
    ConsistencyLevel consistencyLevel = configuration.getQueryOptions().getConsistencyLevel();
    return session instanceof ContinuousPagingSession
        && protocolVersion.compareTo(ProtocolVersion.DSE_V1) >= 0
        && (consistencyLevel == ConsistencyLevel.ONE
            || consistencyLevel == ConsistencyLevel.LOCAL_ONE);
  }

  private void configureExecutor(
      AbstractBulkExecutorBuilder<? extends ReactiveBulkExecutor> builder,
      ExecutionListener executionListener) {
    builder
        .withExecutionListener(executionListener)
        .withMaxInFlightRequests(maxInFlight)
        .withMaxRequestsPerSecond(maxPerSecond)
        .failSafe();
  }
}
