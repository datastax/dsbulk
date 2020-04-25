/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.workflow.commons.settings;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.dsbulk.commons.config.ConfigUtils;
import com.datastax.oss.dsbulk.executor.api.AbstractBulkExecutor;
import com.datastax.oss.dsbulk.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.oss.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.oss.dsbulk.executor.api.listener.MetricsCollectingExecutionListener;
import com.datastax.oss.dsbulk.executor.reactor.ContinuousReactorBulkExecutor;
import com.datastax.oss.dsbulk.executor.reactor.ContinuousReactorBulkExecutorBuilder;
import com.datastax.oss.dsbulk.executor.reactor.DefaultReactorBulkExecutor;
import com.datastax.oss.dsbulk.executor.reactor.DefaultReactorBulkExecutorBuilder;
import com.datastax.oss.dsbulk.executor.reactor.ReactorBulkExecutor;
import com.datastax.oss.dsbulk.executor.reactor.reader.ReactorBulkReader;
import com.datastax.oss.dsbulk.executor.reactor.writer.ReactorBulkWriter;
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
      throw ConfigUtils.convertConfigException(e, "dsbulk.executor");
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
      throw ConfigUtils.convertConfigException(e, "dsbulk.executor.continuousPaging");
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
