/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.listener;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slf4j.event.Level.DEBUG;

import ch.qos.logback.classic.Level;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.datastax.dsbulk.commons.log.LogSink;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(LogInterceptingExtension.class)
class WritesReportingExecutionListenerTest {

  private final LogInterceptor interceptor;
  private final MetricsCollectingExecutionListener delegate;

  public WritesReportingExecutionListenerTest(
      @LogCapture(value = WritesReportingExecutionListener.class, level = DEBUG)
          LogInterceptor interceptor) {
    this.interceptor = interceptor;
    delegate = new MetricsCollectingExecutionListener();
  }

  @Test
  void should_report_writes() {
    Logger logger = LoggerFactory.getLogger(WritesReportingExecutionListener.class);
    WritesReportingExecutionListener listener =
        WritesReportingExecutionListener.builder()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .withLogSink(LogSink.buildFrom(logger::isDebugEnabled, logger::debug))
            .build();

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining("Writes: total: 0, successful: 0, failed: 0, in-flight: 0")
        .hasMessageContaining("Throughput: 0 writes/second, 0.00 mb/second (0.00 kb/write)")
        .hasMessageContaining("Latencies: mean 0.00, 75p 0.00, 99p 0.00, 999p 0.00 milliseconds")
        .hasEventSatisfying(
            event ->
                event.getLevel() == Level.DEBUG
                    && event
                        .getLoggerName()
                        .equals(WritesReportingExecutionListener.class.getName()));

    // simulate 3 writes, 2 successful and 1 failed
    Timer total = delegate.getTotalWritesTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulWritesCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedWritesCounter();
    failed.inc();
    delegate.getInFlightRequestsCounter().inc(42);

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining("Writes: total: 3, successful: 2, failed: 1, in-flight: 42")
        // cannot assert throughput in writes/second as it may vary
        .hasMessageContaining(
            "Latencies: mean 9.99, 75p 10.03, 99p 10.03, 999p 10.03 milliseconds");
  }

  @Test
  void should_report_writes_with_expected_total() {
    WritesReportingExecutionListener listener =
        WritesReportingExecutionListener.builder()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .expectingTotalEvents(3)
            .build();

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining(
            "Writes: total: 0, successful: 0, failed: 0, in-flight: 0, progression: 0%")
        .hasMessageContaining("Throughput: 0 writes/second, 0.00 mb/second (0.00 kb/write)")
        .hasMessageContaining("Latencies: mean 0.00, 75p 0.00, 99p 0.00, 999p 0.00 milliseconds");

    // simulate 3 writes, 2 successful and 1 failed
    Timer total = delegate.getTotalWritesTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulWritesCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedWritesCounter();
    failed.inc();
    delegate.getInFlightRequestsCounter().inc(42);

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining(
            "Writes: total: 3, successful: 2, failed: 1, in-flight: 42, progression: 100%")
        // cannot assert throughput in writes/second as it may vary
        .hasMessageContaining(
            "Latencies: mean 9.99, 75p 10.03, 99p 10.03, 999p 10.03 milliseconds");
  }
}
