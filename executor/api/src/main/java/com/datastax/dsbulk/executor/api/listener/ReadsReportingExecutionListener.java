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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link ExecutionListener} that reports useful metrics about ongoing bulk read operations. It
 * relies on a delegate {@link MetricsCollectingExecutionListener} as its source of metrics.
 */
public class ReadsReportingExecutionListener extends AbstractMetricsReportingExecutionListener
    implements ExecutionListener {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ReadsReportingExecutionListener.class);

  private static final MetricFilter METRIC_FILTER =
      (name, metric) -> name.startsWith("executor/reads/");

  private static final String REPORTER_NAME = "bulk-execution-reads-reporter";

  /**
   * Creates a new builder for this class.
   *
   * @return a new builder.
   */
  public static AbstractMetricsReportingExecutionListenerBuilder<ReadsReportingExecutionListener>
      builder() {
    return new AbstractMetricsReportingExecutionListenerBuilder<ReadsReportingExecutionListener>() {
      @Override
      public ReadsReportingExecutionListener build() {
        Logger l = logger == null ? LOGGER : logger;
        if (scheduler == null) {
          return new ReadsReportingExecutionListener(
              delegate, rateUnit, durationUnit, expectedTotal, l);
        } else {
          return new ReadsReportingExecutionListener(
              delegate, rateUnit, durationUnit, expectedTotal, l, scheduler);
        }
      }
    };
  }

  private final long expectedTotal;
  private final String countMessage;
  private final String throughputMessage;
  private final String latencyMessage;
  private final Timer timer;
  private final Counter failed;
  private final Counter successful;
  private final Counter inFlight;
  private final Meter received;
  private final Logger logger;

  /**
   * Creates a default instance of {@link ReadsReportingExecutionListener}.
   *
   * <p>The instance will express rates in operations per second, and durations in milliseconds.
   */
  public ReadsReportingExecutionListener() {
    this(new MetricsCollectingExecutionListener(), SECONDS, MILLISECONDS, -1, LOGGER);
  }

  /**
   * Creates an instance of {@link ReadsReportingExecutionListener} using the given {@linkplain
   * MetricsCollectingExecutionListener delegate}.
   *
   * <p>The instance will express rates in operations per second, and durations in milliseconds.
   *
   * @param delegate the {@link ReadsReportingExecutionListener} to use as metrics source.
   */
  public ReadsReportingExecutionListener(MetricsCollectingExecutionListener delegate) {
    this(delegate, SECONDS, MILLISECONDS, -1, LOGGER);
  }

  private ReadsReportingExecutionListener(
      MetricsCollectingExecutionListener delegate,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      long expectedTotal,
      Logger logger) {
    super(delegate, REPORTER_NAME, METRIC_FILTER, rateUnit, durationUnit);
    this.expectedTotal = expectedTotal;
    this.logger = logger;
    countMessage = createCountMessageTemplate(expectedTotal);
    throughputMessage = createThroughputMessageTemplate();
    latencyMessage = createLatencyMessageTemplate();
    timer = delegate.getTotalReadsTimer();
    successful = delegate.getSuccessfulReadsCounter();
    failed = delegate.getFailedReadsCounter();
    inFlight = delegate.getInFlightRequestsCounter();
    received = delegate.getBytesReceivedMeter();
  }

  private ReadsReportingExecutionListener(
      MetricsCollectingExecutionListener delegate,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      long expectedTotal,
      Logger logger,
      ScheduledExecutorService scheduler) {
    super(delegate, REPORTER_NAME, METRIC_FILTER, rateUnit, durationUnit, scheduler);
    this.expectedTotal = expectedTotal;
    this.logger = logger;
    countMessage = createCountMessageTemplate(expectedTotal);
    throughputMessage = createThroughputMessageTemplate();
    latencyMessage = createLatencyMessageTemplate();
    timer = delegate.getTotalReadsTimer();
    successful = delegate.getSuccessfulReadsCounter();
    failed = delegate.getFailedReadsCounter();
    inFlight = delegate.getInFlightRequestsCounter();
    received = delegate.getBytesReceivedMeter();
  }

  @Override
  public void report(
      SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {
    Snapshot snapshot = timer.getSnapshot();
    long total = timer.getCount();
    String durationUnit = getDurationUnit();
    String rateUnit = getRateUnit();
    if (expectedTotal < 0) {
      logger.info(
          String.format(
              countMessage, total, successful.getCount(), failed.getCount(), inFlight.getCount()));
    } else {
      float achieved = (float) total / (float) expectedTotal * 100f;
      logger.info(
          String.format(
              countMessage,
              total,
              successful.getCount(),
              failed.getCount(),
              inFlight.getCount(),
              achieved));
    }
    logger.info(
        String.format(
            throughputMessage,
            convertRate(timer.getMeanRate()),
            rateUnit,
            convertRate(received.getMeanRate() / BYTES_PER_MB),
            rateUnit));
    logger.info(
        String.format(
            latencyMessage,
            convertDuration(snapshot.getMean()),
            convertDuration(snapshot.get75thPercentile()),
            convertDuration(snapshot.get99thPercentile()),
            convertDuration(snapshot.get999thPercentile()),
            durationUnit));
  }

  private static String createCountMessageTemplate(long expectedTotal) {
    if (expectedTotal < 0) {
      return "Reads: total: %,d, successful: %,d, failed: %,d, in-flight: %,d";
    } else {
      int numDigits = String.format("%,d", expectedTotal).length();
      return "Reads: "
          + "total: %,"
          + numDigits
          + "d, "
          + "successful: %,"
          + numDigits
          + "d, "
          + "failed: %,d, "
          + "in-flight: %,d, "
          + "progression: %,.0f%%";
    }
  }

  private static String createThroughputMessageTemplate() {
    return "Throughput: %,.0f reads/%s, %,.2f mb/%s";
  }

  private static String createLatencyMessageTemplate() {
    return "Latencies: mean %,.2f, 75p %,.2f, 99p %,.2f, 999p %,.2f %s";
  }
}
