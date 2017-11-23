/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.listener;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private ScheduledExecutorService scheduler;

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
    public Builder reportingReads() {
      metricType = MetricType.READS;
      return this;
    }

    /**
     * Instructs the new reporter to only report writes. By default, reporters report both reads and
     * writes.
     *
     * @return {@code this} (for method chaining).
     */
    public Builder reportingWrites() {
      metricType = MetricType.WRITES;
      return this;
    }

    /**
     * Instructs the new reporter to report both reads and writes. This is the default behavior.
     *
     * @return {@code this} (for method chaining).
     */
    public Builder reportingReadsAndWrites() {
      metricType = MetricType.READS_AND_WRITES;
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
      metricType = MetricType.STATEMENTS;
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
    public Builder withScheduler(ScheduledExecutorService scheduler) {
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
  private final Timer timer;
  private final Counter failed;
  private final Counter successful;
  private final Counter inFlight;

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
    msg = createMessageTemplate(expectedTotal, metricType);
    timer = metricType.timer(delegate);
    successful = metricType.successful(delegate);
    failed = metricType.failed(delegate);
    inFlight = delegate.getInFlightRequestsCounter();
  }

  private MetricsReportingExecutionListener(
      MetricType metricType,
      MetricsCollectingExecutionListener delegate,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      long expectedTotal,
      ScheduledExecutorService scheduler) {
    super(
        delegate.getRegistry(),
        "bulk-execution-reporter",
        metricType.filter(),
        rateUnit,
        durationUnit,
        scheduler);
    this.delegate = delegate;
    this.expectedTotal = expectedTotal;
    msg = createMessageTemplate(expectedTotal, metricType);
    timer = metricType.timer(delegate);
    successful = metricType.successful(delegate);
    failed = metricType.failed(delegate);
    inFlight = delegate.getInFlightRequestsCounter();
  }

  @Override
  public void onExecutionStarted(Statement statement, ExecutionContext context) {
    delegate.onExecutionStarted(statement, context);
  }

  @Override
  public void onWriteRequestStarted(Statement statement, ExecutionContext context) {
    delegate.onWriteRequestStarted(statement, context);
  }

  @Override
  public void onReadRequestStarted(Statement statement, ExecutionContext context) {
    delegate.onReadRequestStarted(statement, context);
  }

  @Override
  public void onWriteRequestSuccessful(Statement statement, ExecutionContext context) {
    delegate.onWriteRequestSuccessful(statement, context);
  }

  @Override
  public void onWriteRequestFailed(Statement statement, Throwable error, ExecutionContext context) {
    delegate.onWriteRequestFailed(statement, error, context);
  }

  @Override
  public void onReadRequestSuccessful(Statement statement, ExecutionContext context) {
    delegate.onReadRequestSuccessful(statement, context);
  }

  @Override
  public void onRowReceived(Row row, ExecutionContext context) {
    delegate.onRowReceived(row, context);
  }

  @Override
  public void onReadRequestFailed(Statement statement, Throwable error, ExecutionContext context) {
    delegate.onReadRequestFailed(statement, error, context);
  }

  @Override
  public void onExecutionSuccessful(Statement statement, ExecutionContext context) {
    delegate.onExecutionSuccessful(statement, context);
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
    report(timer, successful, failed, inFlight);
  }

  /**
   * Performs a report.
   *
   * @param timer the timer recording events.
   * @param successful the counter recording successful events.
   * @param failed the counter recording failed events.
   * @param inFlight the counter recording in-flight requests.
   */
  protected void report(Timer timer, Counter successful, Counter failed, Counter inFlight) {
    Snapshot snapshot = timer.getSnapshot();
    long total = timer.getCount();
    String durationUnit = getDurationUnit();
    String rateUnit = getRateUnit();
    if (expectedTotal < 0) {
      LOGGER.info(
          String.format(
              msg,
              total,
              successful.getCount(),
              failed.getCount(),
              convertRate(timer.getMeanRate()),
              rateUnit,
              convertDuration(snapshot.getMean()),
              convertDuration(snapshot.get75thPercentile()),
              convertDuration(snapshot.get99thPercentile()),
              convertDuration(snapshot.get999thPercentile()),
              durationUnit,
              inFlight.getCount()));
    } else {
      float achieved = (float) total / (float) expectedTotal * 100f;
      LOGGER.info(
          String.format(
              msg,
              total,
              successful.getCount(),
              failed.getCount(),
              convertRate(timer.getMeanRate()),
              rateUnit,
              achieved,
              convertDuration(snapshot.getMean()),
              convertDuration(snapshot.get75thPercentile()),
              convertDuration(snapshot.get99thPercentile()),
              convertDuration(snapshot.get999thPercentile()),
              durationUnit,
              inFlight.getCount()));
    }
  }

  private static String createMessageTemplate(long expectedTotal, MetricType metricType) {
    if (expectedTotal < 0) {
      return metricType.eventName()
          + ": total: %,d, successful: %,d, failed: %,d; %,.0f "
          + metricType.unit()
          + "/%s (mean %,.2f, 75p %,.2f, 99p %,.2f, 999p %,.2f %s, in-flight %,d)";
    } else {
      int numDigits = String.format("%,d", expectedTotal).length();
      return metricType.eventName()
          + ": total: %,"
          + numDigits
          + "d, successful: %,"
          + numDigits
          + "d, failed: %,d; %,.0f "
          + metricType.unit()
          + "/%s, "
          + "progression: %,.0f%% "
          + "(mean %,.2f, 75p %,.2f, 99p %,.2f, 999p %,.2f %s, in-flight %,d)";
    }
  }

  private enum MetricType {
    READS_AND_WRITES {
      @Override
      protected MetricFilter filter() {
        return (name, metric) -> name.startsWith("executor/reads-writes/");
      }

      @Override
      protected Timer timer(MetricsCollectingExecutionListener delegate) {
        return delegate.getReadsWritesTimer();
      }

      @Override
      protected Counter successful(MetricsCollectingExecutionListener delegate) {
        return delegate.getSuccessfulReadsWritesCounter();
      }

      @Override
      protected Counter failed(MetricsCollectingExecutionListener delegate) {
        return delegate.getFailedReadsWritesCounter();
      }

      @Override
      protected String eventName() {
        return "Reads/Writes";
      }

      @Override
      protected String unit() {
        return "reads-writes";
      }
    },

    READS {

      @Override
      protected MetricFilter filter() {
        return (name, metric) -> name.startsWith("executor/reads/");
      }

      @Override
      protected Timer timer(MetricsCollectingExecutionListener delegate) {
        return delegate.getReadsTimer();
      }

      @Override
      protected Counter successful(MetricsCollectingExecutionListener delegate) {
        return delegate.getSuccessfulReadsCounter();
      }

      @Override
      protected Counter failed(MetricsCollectingExecutionListener delegate) {
        return delegate.getFailedReadsCounter();
      }

      @Override
      protected String eventName() {
        return "Reads";
      }

      @Override
      protected String unit() {
        return "reads";
      }
    },

    WRITES {

      @Override
      protected MetricFilter filter() {
        return (name, metric) -> name.startsWith("executor/writes/");
      }

      @Override
      protected Timer timer(MetricsCollectingExecutionListener delegate) {
        return delegate.getWritesTimer();
      }

      @Override
      protected Counter successful(MetricsCollectingExecutionListener delegate) {
        return delegate.getSuccessfulWritesCounter();
      }

      @Override
      protected Counter failed(MetricsCollectingExecutionListener delegate) {
        return delegate.getFailedWritesCounter();
      }

      @Override
      protected String eventName() {
        return "Writes";
      }

      @Override
      protected String unit() {
        return "writes";
      }
    },

    STATEMENTS {

      @Override
      protected MetricFilter filter() {
        return (name, metric) -> name.startsWith("executor/statements/");
      }

      @Override
      protected Timer timer(MetricsCollectingExecutionListener delegate) {
        return delegate.getStatementsTimer();
      }

      @Override
      protected Counter successful(MetricsCollectingExecutionListener delegate) {
        return delegate.getSuccessfulStatementsCounter();
      }

      @Override
      protected Counter failed(MetricsCollectingExecutionListener delegate) {
        return delegate.getFailedStatementsCounter();
      }

      @Override
      protected String eventName() {
        return "Statements";
      }

      @Override
      protected String unit() {
        return "stmts";
      }
    };

    protected abstract MetricFilter filter();

    protected abstract Timer timer(MetricsCollectingExecutionListener delegate);

    protected abstract Counter successful(MetricsCollectingExecutionListener delegate);

    protected abstract Counter failed(MetricsCollectingExecutionListener delegate);

    protected abstract String eventName();

    protected abstract String unit();
  }
}
