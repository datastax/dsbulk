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
import com.datastax.dsbulk.commons.log.LogSink;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link ExecutionListener} that reports useful metrics about ongoing bulk write operations. It
 * relies on a delegate {@link MetricsCollectingExecutionListener} as its source of metrics.
 */
public class WritesReportingExecutionListener extends AbstractMetricsReportingExecutionListener
    implements ExecutionListener {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(WritesReportingExecutionListener.class);

  private static final LogSink DEFAULT_SINK =
      LogSink.buildFrom(LOGGER::isInfoEnabled, LOGGER::info);

  private static final MetricFilter METRIC_FILTER =
      (name, metric) -> name.startsWith("executor/writes/");

  private static final String REPORTER_NAME = "bulk-execution-writes-reporter";

  /**
   * Creates a new builder for this class.
   *
   * @return a new builder.
   */
  public static AbstractMetricsReportingExecutionListenerBuilder<WritesReportingExecutionListener>
      builder() {
    return new AbstractMetricsReportingExecutionListenerBuilder<
        WritesReportingExecutionListener>() {
      @Override
      public WritesReportingExecutionListener build() {
        LogSink s = sink == null ? DEFAULT_SINK : sink;
        if (scheduler == null) {
          return new WritesReportingExecutionListener(
              delegate, rateUnit, durationUnit, expectedTotal, s);
        } else {
          return new WritesReportingExecutionListener(
              delegate, rateUnit, durationUnit, expectedTotal, s, scheduler);
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
  @Nullable private final Meter sent;
  private final LogSink sink;

  /**
   * Creates a default instance of {@link WritesReportingExecutionListener}.
   *
   * <p>The instance will express rates in operations per second, and durations in milliseconds.
   */
  public WritesReportingExecutionListener() {
    this(new MetricsCollectingExecutionListener(), SECONDS, MILLISECONDS, -1, DEFAULT_SINK);
  }

  /**
   * Creates an instance of {@link WritesReportingExecutionListener} using the given {@linkplain
   * MetricsCollectingExecutionListener delegate}.
   *
   * <p>The instance will express rates in operations per second, and durations in milliseconds.
   *
   * @param delegate the {@link WritesReportingExecutionListener} to use as metrics source.
   */
  public WritesReportingExecutionListener(MetricsCollectingExecutionListener delegate) {
    this(delegate, SECONDS, MILLISECONDS, -1, DEFAULT_SINK);
  }

  private WritesReportingExecutionListener(
      MetricsCollectingExecutionListener delegate,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      long expectedTotal,
      LogSink sink) {
    super(delegate, REPORTER_NAME, METRIC_FILTER, rateUnit, durationUnit);
    this.expectedTotal = expectedTotal;
    this.sink = sink;
    countMessage = createCountMessageTemplate(expectedTotal);
    throughputMessage = createThroughputMessageTemplate(delegate.getBytesSentMeter().isPresent());
    latencyMessage = createLatencyMessageTemplate();
    timer = delegate.getTotalWritesTimer();
    successful = delegate.getSuccessfulWritesCounter();
    failed = delegate.getFailedWritesCounter();
    inFlight = delegate.getInFlightRequestsCounter();
    sent = delegate.getBytesSentMeter().orElse(null);
  }

  private WritesReportingExecutionListener(
      MetricsCollectingExecutionListener delegate,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      long expectedTotal,
      LogSink sink,
      ScheduledExecutorService scheduler) {
    super(delegate, REPORTER_NAME, METRIC_FILTER, rateUnit, durationUnit, scheduler);
    this.expectedTotal = expectedTotal;
    this.sink = sink;
    countMessage = createCountMessageTemplate(expectedTotal);
    throughputMessage = createThroughputMessageTemplate(delegate.getBytesSentMeter().isPresent());
    latencyMessage = createLatencyMessageTemplate();
    timer = delegate.getTotalWritesTimer();
    successful = delegate.getSuccessfulWritesCounter();
    failed = delegate.getFailedWritesCounter();
    inFlight = delegate.getInFlightRequestsCounter();
    sent = delegate.getBytesSentMeter().orElse(null);
  }

  @Override
  public void report(
      SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {
    if (!sink.isEnabled()) {
      return;
    }
    Snapshot snapshot = timer.getSnapshot();
    long total = timer.getCount();
    String durationUnit = getDurationUnit();
    String rateUnit = getRateUnit();
    if (expectedTotal < 0) {
      sink.accept(
          String.format(
              countMessage, total, successful.getCount(), failed.getCount(), inFlight.getCount()));
    } else {
      float achieved = (float) total / (float) expectedTotal * 100f;
      sink.accept(
          String.format(
              countMessage,
              total,
              successful.getCount(),
              failed.getCount(),
              inFlight.getCount(),
              achieved));
    }
    double throughput = timer.getMeanRate();
    if (sent != null) {
      double sizeSent = sent.getMeanRate();
      sink.accept(
          String.format(
              throughputMessage,
              convertRate(throughput),
              rateUnit,
              convertRate(sizeSent / BYTES_PER_MB),
              rateUnit,
              throughput == 0 ? 0 : (sizeSent / BYTES_PER_KB) / throughput));
    } else {
      sink.accept(String.format(throughputMessage, convertRate(throughput), rateUnit));
    }
    sink.accept(
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
      return "Writes: total: %,d, successful: %,d, failed: %,d, in-flight: %,d";
    } else {
      int numDigits = String.format("%,d", expectedTotal).length();
      return "Writes: "
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

  private static String createThroughputMessageTemplate(boolean trackThroughputInBytes) {
    if (trackThroughputInBytes) {
      return "Throughput: %,.0f writes/%s, %,.2f mb/%s (%,.2f kb/write)";
    } else {
      return "Throughput: %,.0f writes/%s";
    }
  }

  private static String createLatencyMessageTemplate() {
    return "Latencies: mean %,.2f, 75p %,.2f, 99p %,.2f, 999p %,.2f %s";
  }
}
