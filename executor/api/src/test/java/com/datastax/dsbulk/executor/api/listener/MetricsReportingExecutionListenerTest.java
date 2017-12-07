/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
class MetricsReportingExecutionListenerTest {

  private final LogInterceptor interceptor;
  private final MetricsCollectingExecutionListener delegate;

  public MetricsReportingExecutionListenerTest(
      @LogCapture(MetricsReportingExecutionListener.class) LogInterceptor interceptor) {
    this.interceptor = interceptor;
    delegate = new MetricsCollectingExecutionListener();
  }

  @Test
  void should_report_reads() throws Exception {
    MetricsReportingExecutionListener listener =
        MetricsReportingExecutionListener.builder()
            .reportingReads()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .build();

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining(
            "Reads: total: 0, successful: 0, failed: 0; 0 reads/second (mean 0.00, 75p 0.00, 99p 0.00, 999p 0.00 milliseconds, in-flight 0)");

    // simulate 3 reads, 2 successful and 1 failed
    Timer total = delegate.getReadsTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulReadsCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedReadsCounter();
    failed.inc();
    delegate.getInFlightRequestsCounter().inc(42);

    listener.report();

    assertThat(interceptor).hasMessageContaining("Reads: total: 3, successful: 2, failed: 1");
    // cannot assert throughput in reads/second as it may vary
    assertThat(interceptor)
        .hasMessageContaining(
            "reads/second (mean 9.99, 75p 10.03, 99p 10.03, 999p 10.03 milliseconds, in-flight 42)");
  }

  @Test
  void should_report_reads_with_expected_total() throws Exception {
    MetricsReportingExecutionListener listener =
        MetricsReportingExecutionListener.builder()
            .reportingReads()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .expectingTotalEvents(3)
            .build();

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining(
            "Reads: total: 0, successful: 0, failed: 0; 0 reads/second, progression: 0% (mean 0.00, 75p 0.00, 99p 0.00, 999p 0.00 milliseconds, in-flight 0)");

    // simulate 3 reads, 2 successful and 1 failed
    Timer total = delegate.getReadsTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulReadsCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedReadsCounter();
    failed.inc();
    delegate.getInFlightRequestsCounter().inc(42);

    listener.report();

    assertThat(interceptor).hasMessageContaining("Reads: total: 3, successful: 2, failed: 1");
    // cannot assert throughput in reads/second as it may vary
    assertThat(interceptor)
        .hasMessageContaining(
            "reads/second, progression: 100% (mean 9.99, 75p 10.03, 99p 10.03, 999p 10.03 milliseconds, in-flight 42)");
  }

  @Test
  void should_report_writes() throws Exception {
    MetricsReportingExecutionListener listener =
        MetricsReportingExecutionListener.builder()
            .reportingWrites()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .build();

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining(
            "Writes: total: 0, successful: 0, failed: 0; 0 writes/second (mean 0.00, 75p 0.00, 99p 0.00, 999p 0.00 milliseconds, in-flight 0)");

    // simulate 3 writes, 2 successful and 1 failed
    Timer total = delegate.getWritesTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulWritesCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedWritesCounter();
    failed.inc();
    delegate.getInFlightRequestsCounter().inc(42);

    listener.report();

    assertThat(interceptor).hasMessageContaining("Writes: total: 3, successful: 2, failed: 1");
    // cannot assert throughput in writes/second as it may vary
    assertThat(interceptor)
        .hasMessageContaining(
            "writes/second (mean 9.99, 75p 10.03, 99p 10.03, 999p 10.03 milliseconds, in-flight 42)");
  }

  @Test
  void should_report_writes_with_expected_total() throws Exception {
    MetricsReportingExecutionListener listener =
        MetricsReportingExecutionListener.builder()
            .reportingWrites()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .expectingTotalEvents(3)
            .build();

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining(
            "Writes: total: 0, successful: 0, failed: 0; 0 writes/second, progression: 0% (mean 0.00, 75p 0.00, 99p 0.00, 999p 0.00 milliseconds, in-flight 0)");

    // simulate 3 writes, 2 successful and 1 failed
    Timer total = delegate.getWritesTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulWritesCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedWritesCounter();
    failed.inc();
    delegate.getInFlightRequestsCounter().inc(42);

    listener.report();

    assertThat(interceptor).hasMessageContaining("Writes: total: 3, successful: 2, failed: 1");
    // cannot assert throughput in writes/second as it may vary
    assertThat(interceptor)
        .hasMessageContaining(
            "writes/second, progression: 100% (mean 9.99, 75p 10.03, 99p 10.03, 999p 10.03 milliseconds, in-flight 42)");
  }

  @Test
  void should_report_reads_and_writes() throws Exception {
    MetricsReportingExecutionListener listener =
        MetricsReportingExecutionListener.builder()
            .reportingReadsAndWrites()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .build();

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining(
            "Reads/Writes: total: 0, successful: 0, failed: 0; 0 reads-writes/second (mean 0.00, 75p 0.00, 99p 0.00, 999p 0.00 milliseconds, in-flight 0)");

    // simulate 3 reads/writes, 2 successful and 1 failed
    Timer total = delegate.getReadsWritesTimer();
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
        .hasMessageContaining("Reads/Writes: total: 3, successful: 2, failed: 1");
    // cannot assert throughput in reads/writes/second as it may vary
    assertThat(interceptor)
        .hasMessageContaining(
            "reads-writes/second (mean 9.99, 75p 10.03, 99p 10.03, 999p 10.03 milliseconds, in-flight 42)");
  }

  @Test
  void should_report_reads_and_writes_with_default_constructor() throws Exception {
    MetricsReportingExecutionListener listener = new MetricsReportingExecutionListener();

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining(
            "Reads/Writes: total: 0, successful: 0, failed: 0; 0 reads-writes/second (mean 0.00, 75p 0.00, 99p 0.00, 999p 0.00 milliseconds, in-flight 0)");

    MetricsCollectingExecutionListener delegate =
        (MetricsCollectingExecutionListener) ReflectionUtils.getInternalState(listener, "delegate");
    // simulate 3 reads/writes, 2 successful and 1 failed
    Timer total = delegate.getReadsWritesTimer();
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
        .hasMessageContaining("Reads/Writes: total: 3, successful: 2, failed: 1");
    // cannot assert throughput in reads/writes/second as it may vary
    assertThat(interceptor)
        .hasMessageContaining(
            "reads-writes/second (mean 9.99, 75p 10.03, 99p 10.03, 999p 10.03 milliseconds, in-flight 42)");
  }

  @Test
  void should_report_reads_and_writes_with_expected_total() throws Exception {
    MetricsReportingExecutionListener listener =
        MetricsReportingExecutionListener.builder()
            .reportingReadsAndWrites()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .expectingTotalEvents(3)
            .build();

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining(
            "Reads/Writes: total: 0, successful: 0, failed: 0; 0 reads-writes/second, progression: 0% (mean 0.00, 75p 0.00, 99p 0.00, 999p 0.00 milliseconds, in-flight 0)");

    // simulate 3 reads/writes, 2 successful and 1 failed
    Timer total = delegate.getReadsWritesTimer();
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
        .hasMessageContaining("Reads/Writes: total: 3, successful: 2, failed: 1");
    // cannot assert throughput in reads/writes/second as it may vary
    assertThat(interceptor)
        .hasMessageContaining(
            "reads-writes/second, progression: 100% (mean 9.99, 75p 10.03, 99p 10.03, 999p 10.03 milliseconds, in-flight 42)");
  }

  @Test
  void should_report_statements() throws Exception {
    MetricsReportingExecutionListener listener =
        MetricsReportingExecutionListener.builder()
            .reportingStatements()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .build();

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining(
            "Statements: total: 0, successful: 0, failed: 0; 0 stmts/second (mean 0.00, 75p 0.00, 99p 0.00, 999p 0.00 milliseconds, in-flight 0)");

    // simulate 3 stmts, 2 successful and 1 failed
    Timer total = delegate.getStatementsTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulStatementsCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedStatementsCounter();
    failed.inc();
    delegate.getInFlightRequestsCounter().inc(42);

    listener.report();

    assertThat(interceptor).hasMessageContaining("Statements: total: 3, successful: 2, failed: 1");
    // cannot assert throughput in stmts/second as it may vary
    assertThat(interceptor)
        .hasMessageContaining(
            "stmts/second (mean 9.99, 75p 10.03, 99p 10.03, 999p 10.03 milliseconds, in-flight 42)");
  }

  @Test
  void should_report_statements_with_expected_total() throws Exception {
    MetricsReportingExecutionListener listener =
        MetricsReportingExecutionListener.builder()
            .reportingStatements()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .expectingTotalEvents(3)
            .build();

    listener.report();

    assertThat(interceptor)
        .hasMessageContaining(
            "Statements: total: 0, successful: 0, failed: 0; 0 stmts/second, progression: 0% (mean 0.00, 75p 0.00, 99p 0.00, 999p 0.00 milliseconds, in-flight 0)");

    // simulate 3 stmts, 2 successful and 1 failed
    Timer total = delegate.getStatementsTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulStatementsCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedStatementsCounter();
    failed.inc();
    delegate.getInFlightRequestsCounter().inc(42);

    listener.report();

    assertThat(interceptor).hasMessageContaining("Statements: total: 3, successful: 2, failed: 1");
    // cannot assert throughput in stmts/second as it may vary
    assertThat(interceptor)
        .hasMessageContaining(
            "stmts/second, progression: 100% (mean 9.99, 75p 10.03, 99p 10.03, 999p 10.03 milliseconds, in-flight 42)");
  }
}
