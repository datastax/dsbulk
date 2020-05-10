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
import com.datastax.oss.dsbulk.executor.api.BulkExecutor;
import com.datastax.oss.dsbulk.executor.api.BulkExecutorBuilder;
import com.datastax.oss.dsbulk.executor.api.BulkExecutorBuilderFactory;
import com.datastax.oss.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.oss.dsbulk.executor.api.listener.MetricsCollectingExecutionListener;
import com.datastax.oss.dsbulk.executor.api.reader.BulkReader;
import com.datastax.oss.dsbulk.executor.api.writer.BulkWriter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorSettings {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorSettings.class);

  private final Config config;

  private int maxPerSecond;
  private int maxInFlight;
  private boolean continuousPagingEnabled;

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
        if (ConfigUtils.hasUserOverride(config, "continuousPaging.maxConcurrentQueries")) {
          LOGGER.warn(
              "Setting executor.continuousPaging.maxConcurrentQueries has been removed and is not honored anymore; "
                  + "please remove it from your configuration. To configure query concurrency, please use "
                  + "--dsbulk.executor.maxConcurrentQueries instead.");
        }
      }
    } catch (ConfigException e) {
      throw ConfigUtils.convertConfigException(e, "dsbulk.executor.continuousPaging");
    }
  }

  @NonNull
  public BulkWriter newWriteExecutor(
      @NonNull CqlSession session, @NonNull ExecutionListener executionListener) {
    return newBulkExecutor(session, executionListener, false, false);
  }

  @NonNull
  public BulkReader newReadExecutor(
      @NonNull CqlSession session,
      @NonNull MetricsCollectingExecutionListener executionListener,
      boolean searchQuery) {
    return newBulkExecutor(session, executionListener, true, searchQuery);
  }

  @NonNull
  protected BulkExecutor newBulkExecutor(
      @NonNull CqlSession session,
      @NonNull ExecutionListener executionListener,
      boolean read,
      boolean searchQuery) {
    boolean useContinuousPagingForReads = read && checkContinuousPaging(session, searchQuery);
    ServiceLoader<BulkExecutorBuilderFactory> loader =
        ServiceLoader.load(BulkExecutorBuilderFactory.class);
    BulkExecutorBuilderFactory builderFactory = loader.iterator().next();
    BulkExecutorBuilder<?> builder = builderFactory.create(session, useContinuousPagingForReads);
    builder
        .withExecutionListener(executionListener)
        .withMaxInFlightRequests(maxInFlight)
        .withMaxRequestsPerSecond(maxPerSecond)
        .failSafe();
    return builder.build();
  }

  protected boolean checkContinuousPaging(@NonNull CqlSession session, boolean searchQuery) {
    if (continuousPagingEnabled) {
      if (searchQuery) {
        LOGGER.warn(
            "Continuous paging is enabled but is not compatible with search queries; disabling.");
        return false;
      }
      if (continuousPagingAvailable(session)) {
        return true;
      } else {
        LOGGER.warn(
            "Continuous paging is not available, read performance will not be optimal. "
                + "Check your remote DSE cluster configuration, and ensure that "
                + "the configured consistency level is either ONE or LOCAL_ONE.");
      }
    } else {
      LOGGER.debug("Continuous paging was disabled by configuration.");
    }
    return false;
  }

  protected boolean continuousPagingAvailable(@NonNull CqlSession session) {
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
}
