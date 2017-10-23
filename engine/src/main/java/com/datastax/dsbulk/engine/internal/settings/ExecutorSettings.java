/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

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
import com.datastax.dsbulk.executor.api.listener.MetricsCollectingExecutionListener;
import com.datastax.dsbulk.executor.api.reader.ReactorBulkReader;
import com.datastax.dsbulk.executor.api.throttling.DynamicRateLimiter;
import com.datastax.dsbulk.executor.api.throttling.DynamicRateLimiterBuilder;
import com.datastax.dsbulk.executor.api.throttling.FixedRateLimiter;
import com.datastax.dsbulk.executor.api.writer.ReactorBulkWriter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class ExecutorSettings implements SettingsValidator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorSettings.class);

  private final LoaderConfig config;

  ExecutorSettings(LoaderConfig config) {
    this.config = config;
  }

  public ReactorBulkWriter newWriteExecutor(
      Session session, MetricsCollectingExecutionListener executionListener) {
    return newBulkExecutor(session, executionListener, WorkflowType.LOAD);
  }

  public ReactorBulkReader newReadExecutor(
      Session session, MetricsCollectingExecutionListener executionListener) {
    return newBulkExecutor(session, executionListener, WorkflowType.UNLOAD);
  }

  private ReactorBulkExecutor newBulkExecutor(
      Session session,
      MetricsCollectingExecutionListener executionListener,
      WorkflowType workflowType) {
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
      config.getThreads("maxThreads");
      config.getInt("maxInFlight");
      if (config.getBoolean("continuousPaging.enabled")) {
        config.getInt("continuousPaging.pageSize");
        config.getEnum(ContinuousPagingOptions.PageUnit.class, "continuousPaging.pageUnit");
        config.getInt("continuousPaging.maxPages");
        config.getInt("continuousPaging.maxPagesPerSecond");
      }
      if (config.getBoolean("throttling.enabled")) {
        config.getEnum(ThrottlingMode.class, "throttling.mode");
        config.getEnum(ThrottlingStatProvider.class, "throttling.statProvider");
        config.getInt("throttling.rate");
        config.getLong("throttling.minThreshold");
        config.getLong("throttling.maxThreshold");
        config.getDouble("throttling.upMultiplier");
        config.getDouble("throttling.downMultiplier");
        config.getDuration("throttling.checkInterval");
        config.getDuration("throttling.warmUpPeriod");
      }
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "executor");
    }
  }

  public int getMaxInFlight() {
    return config.getInt("maxInFlight");
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
      MetricsCollectingExecutionListener executionListener) {
    int threads = config.getThreads("maxThreads");
    // will be closed when the Bulk Executor gets closed
    Executor executor =
        new ThreadPoolExecutor(
            0,
            threads,
            60,
            SECONDS,
            new SynchronousQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("bulk-executor-%0,2d").build(),
            new ThreadPoolExecutor.CallerRunsPolicy());
    builder.withExecutor(executor).withExecutionListener(executionListener).failSafe();
    int maxInFlight = config.getInt("maxInFlight");
    builder.withMaxInFlightRequests(maxInFlight);
    boolean throttlingEnabled = config.getBoolean("throttling.enabled");
    if (throttlingEnabled) {
      LoaderConfig throttlingConfig = config.getConfig("throttling");
      ThrottlingMode throttlingMode = throttlingConfig.getEnum(ThrottlingMode.class, "mode");
      int rate = throttlingConfig.getInt("rate");
      switch (throttlingMode) {
        case FIXED:
          builder.withRateLimiter(new FixedRateLimiter(rate));
          break;
        case DYNAMIC:
          DynamicRateLimiterBuilder rateLimiterBuilder =
              DynamicRateLimiter.builder(rate)
                  .withCheckInterval(throttlingConfig.getDuration("checkInterval"))
                  .withWarmUpPeriod(throttlingConfig.getDuration("warmUpPeriod"))
                  .withRateProvider(
                      () -> executionListener.getReadsWritesTimer().getOneMinuteRate())
                  .withExponentialBackOff(
                      throttlingConfig.getDouble("upMultiplier"),
                      throttlingConfig.getDouble("downMultiplier"));
          ThrottlingStatProvider provider =
              throttlingConfig.getEnum(ThrottlingStatProvider.class, "statProvider");
          switch (provider) {
            case IN_FLIGHT:
              if (maxInFlight > 0 && throttlingConfig.getInt("maxThreshold") >= maxInFlight) {
                throw new IllegalStateException(
                    String.format(
                        "executor.throttling.maxThreshold must be lesser than executor.maxInFlight, got %s >= %s",
                        throttlingConfig.getInt("maxThreshold"), maxInFlight));
              }
              rateLimiterBuilder =
                  rateLimiterBuilder
                      .withFixedThresholds(
                          throttlingConfig.getLong("minThreshold"),
                          throttlingConfig.getLong("maxThreshold"))
                      .withStatProvider(
                          () -> executionListener.getInFlightRequestsCounter().getCount());
              break;
            case LATENCY:
              rateLimiterBuilder =
                  rateLimiterBuilder
                      .withFixedThresholds(
                          MILLISECONDS.toNanos(throttlingConfig.getLong("minThreshold")),
                          MILLISECONDS.toNanos(throttlingConfig.getLong("maxThreshold")))
                      .withStatProvider(
                          () ->
                              executionListener
                                  .getReadsWritesTimer()
                                  .getSnapshot()
                                  .get999thPercentile());
              break;
          }
          builder.withRateLimiter(rateLimiterBuilder.build());
          break;
      }
    } else {
      builder.withoutRateLimiter();
    }
  }

  private enum ThrottlingMode {
    FIXED,
    DYNAMIC
  }

  private enum ThrottlingStatProvider {
    IN_FLIGHT,
    LATENCY
  }
}
