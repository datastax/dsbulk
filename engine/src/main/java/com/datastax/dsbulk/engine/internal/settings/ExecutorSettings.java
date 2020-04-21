/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.executor.api.AbstractBulkExecutor;
import com.datastax.dsbulk.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.listener.MetricsCollectingExecutionListener;
import com.datastax.dsbulk.executor.reactor.ContinuousReactorBulkExecutor;
import com.datastax.dsbulk.executor.reactor.ContinuousReactorBulkExecutorBuilder;
import com.datastax.dsbulk.executor.reactor.DefaultReactorBulkExecutor;
import com.datastax.dsbulk.executor.reactor.DefaultReactorBulkExecutorBuilder;
import com.datastax.dsbulk.executor.reactor.ReactorBulkExecutor;
import com.datastax.dsbulk.executor.reactor.reader.ReactorBulkReader;
import com.datastax.dsbulk.executor.reactor.writer.ReactorBulkWriter;
import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorSettings {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorSettings.class);

  private final Config config;

  private int maxPerSecond;
  private int maxInFlight;
  private boolean continuousPagingEnabled;
  private int maxConcurrentQueries;

  ExecutorSettings(Config config) {
    this.config = config;
  }

  public void init() {
    try {
      maxPerSecond = config.getInt("maxPerSecond");
      maxInFlight = config.getInt("maxInFlight");
    } catch (ConfigException e) {
      throw BulkConfigurationException.fromTypeSafeConfigException(e, "dsbulk.executor");
    }
    Config continuousPagingConfig = config.getConfig("continuousPaging");
    try {
      continuousPagingEnabled = continuousPagingConfig.getBoolean("enabled");
      // deprecated continuous paging options are now parsed in DriverSettings where they are
      // converted into driver options
      if (continuousPagingEnabled) {
        maxConcurrentQueries = continuousPagingConfig.getInt("maxConcurrentQueries");
      }
    } catch (ConfigException e) {
      throw BulkConfigurationException.fromTypeSafeConfigException(
          e, "dsbulk.executor.continuousPaging");
    }
  }

  public Optional<Integer> getMaxInFlight() {
    return maxInFlight > 0 ? Optional.of(maxInFlight) : Optional.empty();
  }

  public Optional<Integer> getMaxConcurrentQueries() {
    return maxConcurrentQueries > 0 ? Optional.of(maxConcurrentQueries) : Optional.empty();
  }

  public ReactorBulkWriter newWriteExecutor(
      CqlSession session, ExecutionListener executionListener) {
    return newBulkExecutor(session, executionListener, false, false);
  }

  public ReactorBulkReader newReadExecutor(
      CqlSession session,
      MetricsCollectingExecutionListener executionListener,
      boolean searchQuery) {
    return newBulkExecutor(session, executionListener, true, searchQuery);
  }

  private ReactorBulkExecutor newBulkExecutor(
      CqlSession session, ExecutionListener executionListener, boolean read, boolean searchQuery) {
    if (read) {
      if (continuousPagingEnabled) {
        if (searchQuery) {
          LOGGER.warn(
              "Continuous paging is enabled but is not compatible with search queries; disabling.");
          return newDefaultExecutor(session, executionListener);
        }
        if (continuousPagingAvailable(session)) {
          return newContinuousExecutor(session, executionListener);
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
      CqlSession session, ExecutionListener executionListener) {
    DefaultReactorBulkExecutorBuilder builder = DefaultReactorBulkExecutor.builder(session);
    return configureExecutor(builder, executionListener).build();
  }

  private ReactorBulkExecutor newContinuousExecutor(
      CqlSession session, ExecutionListener executionListener) {
    ContinuousReactorBulkExecutorBuilder builder =
        ContinuousReactorBulkExecutor.continuousPagingBuilder(session);
    return configureExecutor(builder, executionListener)
        .withMaxInFlightQueries(maxConcurrentQueries)
        .build();
  }

  private boolean continuousPagingAvailable(CqlSession session) {
    ProtocolVersion protocolVersion = session.getContext().getProtocolVersion();
    if (protocolVersion.getCode() >= DseProtocolVersion.DSE_V1.getCode()) {
      DefaultConsistencyLevel consistencyLevel =
          DefaultConsistencyLevel.valueOf(
              session
                  .getContext()
                  .getConfig()
                  .getDefaultProfile()
                  .getString(DefaultDriverOption.REQUEST_CONSISTENCY));
      return consistencyLevel == DefaultConsistencyLevel.ONE
          || consistencyLevel == DefaultConsistencyLevel.LOCAL_ONE;
    }
    return false;
  }

  private <T extends AbstractBulkExecutor> AbstractBulkExecutorBuilder<T> configureExecutor(
      AbstractBulkExecutorBuilder<T> builder, ExecutionListener executionListener) {
    return builder
        .withExecutionListener(executionListener)
        .withMaxInFlightRequests(maxInFlight)
        .withMaxRequestsPerSecond(maxPerSecond)
        .failSafe();
  }
}
