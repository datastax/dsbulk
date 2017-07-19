/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.listener;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.Statement;
import com.datastax.loader.executor.api.exception.BulkExecutionException;
import com.datastax.loader.executor.api.result.Result;
import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * An {@link ExecutionListener} that reports useful metrics about ongoing bulk operations. It relies
 * on a delegate {@link MetricsCollectingExecutionListener} as its source of metrics.
 */
public class MetricsReportingExecutionListener extends ScheduledReporter
    implements ExecutionListener {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(MetricsReportingExecutionListener.class);

  /**
   * Creates a new builder for this class.
   *
   * @return a new {@link MetricsReportingExecutionListener.Builder}.
   */
  public static MetricsReportingExecutionListener.Builder builder() {
    return new MetricsReportingExecutionListener.Builder();
  }

  /** A builder for {@link MetricsReportingExecutionListener}. */
  public static class Builder {

    private MetricsCollectingExecutionListener delegate;
    private long expectedTotal = -1;
    private TimeUnit rateUnit = SECONDS;
    private TimeUnit durationUnit = MILLISECONDS;
    private MetricType metricType = MetricType.READS_AND_WRITES;
    private ScheduledThreadPoolExecutor scheduler;

    private Builder() {}

    /**
     * Instructs the new reporter to use the given {@link MetricsCollectingExecutionListener
     * delegate} as is source of metrics.
     *
     * <p>If this method is not called, a newly-allocated {@link MetricsCollectingExecutionListener}
     * will be used.
     *
     * @param delegate the {@link MetricsReportingExecutionListener} to use as metrics source.
     * @return {@code this} (for method chaining).
     */
    public Builder extractingMetricsFrom(MetricsCollectingExecutionListener delegate) {
      this.delegate = Objects.requireNonNull(delegate);
      return this;
    }

    /**
     * Instructs the new reporter to only report reads. By default, reporters report both reads and
     * writes.
     *
     * @return {@code this} (for method chaining).
     */
    public Builder reportingReadsOnly() {
      this.metricType = MetricType.READS;
      return this;
    }

    /**
     * Instructs the new reporter to only report writes. By default, reporters report both reads and
     * writes.
     *
     * @return {@code this} (for method chaining).
     */
    public Builder reportingWritesOnly() {
      this.metricType = MetricType.WRITES;
      return this;
    }

    /**
     * Instructs the new reporter to report both reads and writes. This is the default behavior.
     *
     * @return {@code this} (for method chaining).
     */
    public Builder reportingReadsAndWrites() {
      this.metricType = MetricType.READS_AND_WRITES;
      return this;
    }

    /**
     * Instructs the new reporter to only report statements. By default, reporters report both reads
     * and writes. When reporting statements, batch statements and read statements count for just
     * one event.
     *
     * @return {@code this} (for method chaining).
     */
    public Builder reportingStatements() {
      this.metricType = MetricType.STATEMENTS;
      return this;
    }

    /**
     * The total number of expected events.
     *
     * <p>If this number is set, the reporter will also print a percentage of the overall
     * progression.
     *
     * @param expectedTotal the total number of expected events.
     * @return {@code this} (for method chaining)
     */
    public Builder expectingTotalEvents(long expectedTotal) {
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
    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time.
     * @return {@code this} (for method chaining)
     */
    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    /**
     * Use the given {@link ScheduledThreadPoolExecutor scheduler} to schedule periodic reports.
     *
     * <p>If this method is not called, a default scheduler is created.
     *
     * @param scheduler the {@link ScheduledThreadPoolExecutor scheduler} to use.
     * @return {@code this} (for method chaining)
     */
    public Builder withScheduler(ScheduledThreadPoolExecutor scheduler) {
      this.scheduler = scheduler;
      return this;
    }

    /**
     * Builds a new instance of {@link MetricsReportingExecutionListener}.
     *
     * @return a new instance of {@link MetricsReportingExecutionListener}.
     */
    public MetricsReportingExecutionListener build() {
      return scheduler == null
          ? new MetricsReportingExecutionListener(
              metricType,
              delegate == null ? new MetricsCollectingExecutionListener() : delegate,
              rateUnit,
              durationUnit,
              expectedTotal)
          : new MetricsReportingExecutionListener(
              metricType,
              delegate == null ? new MetricsCollectingExecutionListener() : delegate,
              rateUnit,
              durationUnit,
              expectedTotal,
              scheduler);
    }
  }

  private final MetricsCollectingExecutionListener delegate;
  private final long expectedTotal;
  private final String msg;
  private final Timer failedTimer;
  private final Timer totalTimer;
  private final Timer successfulTimer;

  /**
   * Creates a default instance of {@link MetricsReportingExecutionListener}.
   *
   * <p>The instance will report both reads and writes and express rates in operations per second,
   * and durations in milliseconds.
   */
  public MetricsReportingExecutionListener() {
    this(
        MetricType.READS_AND_WRITES,
        new MetricsCollectingExecutionListener(),
        SECONDS,
        MILLISECONDS,
        -1);
  }

  /**
   * Creates an instance of {@link MetricsReportingExecutionListener} using the given {@link
   * MetricsCollectingExecutionListener delegate}.
   *
   * <p>The instance will report both reads and writes and express rates in operations per second,
   * and durations in milliseconds.
   *
   * @param delegate the {@link MetricsReportingExecutionListener} to use as metrics source.
   */
  public MetricsReportingExecutionListener(MetricsCollectingExecutionListener delegate) {
    this(MetricType.READS_AND_WRITES, delegate, SECONDS, MILLISECONDS, -1);
  }

  private MetricsReportingExecutionListener(
      MetricType metricType,
      MetricsCollectingExecutionListener delegate,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      long expectedTotal) {
    super(
        delegate.getRegistry(),
        "bulk-execution-reporter",
        metricType.filter(),
        rateUnit,
        durationUnit);
    this.delegate = delegate;
    this.expectedTotal = expectedTotal;
    this.msg = createMessageTemplate(expectedTotal, metricType.eventName());
    this.totalTimer = metricType.totalTimer(delegate);
    this.successfulTimer = metricType.successfulTimer(delegate);
    this.failedTimer = metricType.failedTimer(delegate);
  }

  private MetricsReportingExecutionListener(
      MetricType metricType,
      MetricsCollectingExecutionListener delegate,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      long expectedTotal,
      ScheduledThreadPoolExecutor scheduler) {
    super(
        delegate.getRegistry(),
        "bulk-execution-reporter",
        metricType.filter(),
        rateUnit,
        durationUnit,
        scheduler);
    this.delegate = delegate;
    this.expectedTotal = expectedTotal;
    this.msg = createMessageTemplate(expectedTotal, metricType.eventName());
    this.totalTimer = metricType.totalTimer(delegate);
    this.successfulTimer = metricType.successfulTimer(delegate);
    this.failedTimer = metricType.failedTimer(delegate);
  }

  @Override
  public void onExecutionStarted(Statement statement, ExecutionContext context) {
    delegate.onExecutionStarted(statement, context);
  }

  @Override
  public void onResultReceived(Result result, ExecutionContext context) {
    delegate.onResultReceived(result, context);
  }

  @Override
  public void onExecutionCompleted(Statement statement, ExecutionContext context) {
    delegate.onExecutionCompleted(statement, context);
  }

  @Override
  public void onExecutionFailed(BulkExecutionException exception, ExecutionContext context) {
    delegate.onExecutionFailed(exception, context);
  }

  @Override
  public final void report(
      SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {
    report(totalTimer, successfulTimer, failedTimer);
  }

  /**
   * Performs a report.
   *
   * @param totalTimer the timer recording all events, both successful and failed.
   * @param successfulTimer the timer recording successful events only.
   * @param failedTimer the timer recording failed events only.
   */
  protected void report(Timer totalTimer, Timer successfulTimer, Timer failedTimer) {
    Snapshot snapshot = totalTimer.getSnapshot();
    long total = totalTimer.getCount();
    long successful = successfulTimer.getCount();
    long failed = failedTimer.getCount();
    String durationUnit = getDurationUnit();
    String rateUnit = getRateUnit();
    if (expectedTotal < 0) {
      LOGGER.info(
          String.format(
              msg,
              total,
              successful,
              failed,
              convertRate(totalTimer.getMeanRate()),
              rateUnit,
              convertDuration(snapshot.getMean()),
              convertDuration(snapshot.get75thPercentile()),
              convertDuration(snapshot.get99thPercentile()),
              durationUnit));
    } else {
      float achieved = (float) total / (float) expectedTotal * 100f;
      LOGGER.info(
          String.format(
              msg,
              total,
              successful,
              failed,
              convertRate(totalTimer.getMeanRate()),
              rateUnit,
              achieved,
              convertDuration(snapshot.getMean()),
              convertDuration(snapshot.get75thPercentile()),
              convertDuration(snapshot.get99thPercentile()),
              durationUnit));
    }
  }

  private static String createMessageTemplate(long expectedTotal, String eventName) {
    if (expectedTotal < 0) {
      return "Total: %,d, successful: %,d, failed: %,d; %,.0f "
          + eventName
          + "/%s (mean %,.2f, 75p %,.2f, 99p %,.2f %s)";
    } else {
      int numDigits = expectedTotal > 0 ? String.format("%,d", expectedTotal).length() : -1;
      return "Total: %,"
          + numDigits
          + "d, successful: %,"
          + numDigits
          + "d, failed: %,d; %,.0f "
          + eventName
          + "/%s, progression: %,.0f%%"
          + " (mean %,.2f, 75p %,.2f, 99p %,.2f %s)";
    }
  }

  private enum MetricType {
    READS_AND_WRITES {

      @Override
      protected MetricFilter filter() {
        return (name, metric) -> name.endsWith("-operations-timer");
      }

      @Override
      protected Timer totalTimer(MetricsCollectingExecutionListener delegate) {
        return delegate.getTotalOperationsTimer();
      }

      @Override
      protected Timer successfulTimer(MetricsCollectingExecutionListener delegate) {
        return delegate.getSuccessfulOperationsTimer();
      }

      @Override
      protected Timer failedTimer(MetricsCollectingExecutionListener delegate) {
        return delegate.getFailedOperationsTimer();
      }

      @Override
      protected String eventName() {
        return "ops";
      }
    },

    READS {

      @Override
      protected MetricFilter filter() {
        return (name, metric) -> name.endsWith("-reads-timer");
      }

      @Override
      protected Timer totalTimer(MetricsCollectingExecutionListener delegate) {
        return delegate.getTotalReadsTimer();
      }

      @Override
      protected Timer successfulTimer(MetricsCollectingExecutionListener delegate) {
        return delegate.getSuccessfulReadsTimer();
      }

      @Override
      protected Timer failedTimer(MetricsCollectingExecutionListener delegate) {
        return delegate.getFailedReadsTimer();
      }

      @Override
      protected String eventName() {
        return "reads";
      }
    },

    WRITES {

      @Override
      protected MetricFilter filter() {
        return (name, metric) -> name.endsWith("-writes-timer");
      }

      @Override
      protected Timer totalTimer(MetricsCollectingExecutionListener delegate) {
        return delegate.getTotalWritesTimer();
      }

      @Override
      protected Timer successfulTimer(MetricsCollectingExecutionListener delegate) {
        return delegate.getSuccessfulWritesTimer();
      }

      @Override
      protected Timer failedTimer(MetricsCollectingExecutionListener delegate) {
        return delegate.getFailedWritesTimer();
      }

      @Override
      protected String eventName() {
        return "writes";
      }
    },

    STATEMENTS {

      @Override
      protected MetricFilter filter() {
        return (name, metric) -> name.endsWith("-statements-timer");
      }

      @Override
      protected Timer totalTimer(MetricsCollectingExecutionListener delegate) {
        return delegate.getTotalStatementsTimer();
      }

      @Override
      protected Timer successfulTimer(MetricsCollectingExecutionListener delegate) {
        return delegate.getSuccessfulStatementsTimer();
      }

      @Override
      protected Timer failedTimer(MetricsCollectingExecutionListener delegate) {
        return delegate.getFailedStatementsTimer();
      }

      @Override
      protected String eventName() {
        return "stmts";
      }
    };

    protected abstract MetricFilter filter();

    protected abstract Timer totalTimer(MetricsCollectingExecutionListener delegate);

    protected abstract Timer successfulTimer(MetricsCollectingExecutionListener delegate);

    protected abstract Timer failedTimer(MetricsCollectingExecutionListener delegate);

    protected abstract String eventName();
  }
}
