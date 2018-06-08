/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.listener;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.dsbulk.commons.log.LogSink;
import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A builder for {@link AbstractMetricsReportingExecutionListener}.
 *
 * @param <T> the actual type that this builder builds.
 */
public abstract class AbstractMetricsReportingExecutionListenerBuilder<
    T extends AbstractMetricsReportingExecutionListener> {

  MetricsCollectingExecutionListener delegate;
  long expectedTotal = -1;
  TimeUnit rateUnit = SECONDS;
  TimeUnit durationUnit = MILLISECONDS;
  ScheduledExecutorService scheduler;
  LogSink sink;

  AbstractMetricsReportingExecutionListenerBuilder() {}

  /**
   * Instructs the new reporter to use the given {@linkplain MetricsCollectingExecutionListener
   * delegate} as is source of metrics.
   *
   * <p>If this method is not called, a newly-allocated {@link MetricsCollectingExecutionListener}
   * will be used.
   *
   * @param delegate the {@link ReadsReportingExecutionListener} to use as metrics source.
   * @return {@code this} (for method chaining).
   */
  public AbstractMetricsReportingExecutionListenerBuilder<T> extractingMetricsFrom(
      MetricsCollectingExecutionListener delegate) {
    this.delegate = Objects.requireNonNull(delegate);
    return this;
  }

  /**
   * The total number of expected events.
   *
   * <p>If this number is set, the reporter will also print a percentage of the overall progression.
   *
   * @param expectedTotal the total number of expected events.
   * @return {@code this} (for method chaining)
   */
  public AbstractMetricsReportingExecutionListenerBuilder<T> expectingTotalEvents(
      long expectedTotal) {
    Preconditions.checkArgument(expectedTotal > 0);
    this.expectedTotal = expectedTotal;
    return this;
  }

  /**
   * Convert rates to the given time unit.
   *
   * @param rateUnit a unit of time.
   * @return {@code this} (for method chaining)
   */
  public AbstractMetricsReportingExecutionListenerBuilder<T> convertRatesTo(TimeUnit rateUnit) {
    this.rateUnit = rateUnit;
    return this;
  }

  /**
   * Convert durations to the given time unit.
   *
   * @param durationUnit a unit of time.
   * @return {@code this} (for method chaining)
   */
  public AbstractMetricsReportingExecutionListenerBuilder<T> convertDurationsTo(
      TimeUnit durationUnit) {
    this.durationUnit = durationUnit;
    return this;
  }

  /**
   * Use the given {@linkplain ScheduledThreadPoolExecutor scheduler} to schedule periodic reports.
   *
   * <p>If this method is not called, a default scheduler is created.
   *
   * @param scheduler the {@link ScheduledThreadPoolExecutor scheduler} to use.
   * @return {@code this} (for method chaining)
   */
  public AbstractMetricsReportingExecutionListenerBuilder<T> withScheduler(
      ScheduledExecutorService scheduler) {
    this.scheduler = scheduler;
    return this;
  }

  /**
   * Use the given {@link LogSink} to log messages.
   *
   * <p>If this method is not called, then a default log sink is used.
   *
   * @param sink the {@link LogSink} to use.
   * @return {@code this} (for method chaining).
   */
  public AbstractMetricsReportingExecutionListenerBuilder<T> withLogSink(LogSink sink) {
    this.sink = sink;
    return this;
  }

  /**
   * Builds a new instance of {@link AbstractMetricsReportingExecutionListener}.
   *
   * @return a new instance of {@link AbstractMetricsReportingExecutionListener}.
   */
  public abstract T build();
}
