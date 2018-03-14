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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
class ReadsAndWritesReportingExecutionListenerTest {

  private final LogInterceptor interceptor;
  private final MetricsCollectingExecutionListener delegate;

  public ReadsAndWritesReportingExecutionListenerTest(
      @LogCapture(ReadsAndWritesReportingExecutionListener.class) LogInterceptor interceptor) {
    this.interceptor = interceptor;
    delegate = new MetricsCollectingExecutionListener();
  }

  @Test
  void should_report_reads_and_writes() {
    ReadsAndWritesReportingExecutionListener listener =
        ReadsAndWritesReportingExecutionListener.builder()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .build();

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining("Reads/Writes: total: 0, successful: 0, failed: 0, in-flight: 0")
        .hasMessageContaining(
            "Throughput: 0 reads-writes/second, 0.00 mb/second sent, 0.00 mb/second received")
        .hasMessageContaining("Latencies: mean 0.00, 75p 0.00, 99p 0.00, 999p 0.00 milliseconds");

    // simulate 3 reads/writes, 2 successful and 1 failed
    Timer total = delegate.getTotalReadsWritesTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulReadsWritesCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedReadsWritesCounter();
    failed.inc();
    delegate.getInFlightRequestsCounter().inc(42);

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining("Reads/Writes: total: 3, successful: 2, failed: 1, in-flight: 42")
        // cannot assert throughput in reads-writes/second as it may vary
        .hasMessageContaining(
            "Latencies: mean 9.99, 75p 10.03, 99p 10.03, 999p 10.03 milliseconds");
  }

  @Test
  void should_report_reads_and_writes_with_default_constructor() {
    ReadsAndWritesReportingExecutionListener listener =
        new ReadsAndWritesReportingExecutionListener();

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining("Reads/Writes: total: 0, successful: 0, failed: 0, in-flight: 0")
        .hasMessageContaining(
            "Throughput: 0 reads-writes/second, 0.00 mb/second sent, 0.00 mb/second received")
        .hasMessageContaining("Latencies: mean 0.00, 75p 0.00, 99p 0.00, 999p 0.00 milliseconds");

    MetricsCollectingExecutionListener delegate =
        (MetricsCollectingExecutionListener) ReflectionUtils.getInternalState(listener, "delegate");
    // simulate 3 reads/writes, 2 successful and 1 failed
    Timer total = delegate.getTotalReadsWritesTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulReadsWritesCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedReadsWritesCounter();
    failed.inc();
    delegate.getInFlightRequestsCounter().inc(42);

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining("Reads/Writes: total: 3, successful: 2, failed: 1, in-flight: 42")
        // cannot assert throughput in reads-writes/second as it may vary
        .hasMessageContaining(
            "Latencies: mean 9.99, 75p 10.03, 99p 10.03, 999p 10.03 milliseconds");
  }

  @Test
  void should_report_reads_and_writes_with_expected_total() {
    ReadsAndWritesReportingExecutionListener listener =
        ReadsAndWritesReportingExecutionListener.builder()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .expectingTotalEvents(3)
            .build();

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining("Reads/Writes: total: 0, successful: 0, failed: 0, in-flight: 0")
        .hasMessageContaining(
            "Throughput: 0 reads-writes/second, 0.00 mb/second sent, 0.00 mb/second received")
        .hasMessageContaining("Latencies: mean 0.00, 75p 0.00, 99p 0.00, 999p 0.00 milliseconds");

    // simulate 3 reads/writes, 2 successful and 1 failed
    Timer total = delegate.getTotalReadsWritesTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulReadsWritesCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedReadsWritesCounter();
    failed.inc();
    delegate.getInFlightRequestsCounter().inc(42);

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining("Reads/Writes: total: 3, successful: 2, failed: 1, in-flight: 42")
        // cannot assert throughput in reads-writes/second as it may vary
        .hasMessageContaining(
            "Latencies: mean 9.99, 75p 10.03, 99p 10.03, 999p 10.03 milliseconds");
  }
}
