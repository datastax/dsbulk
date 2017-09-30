/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.listener;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.LoggerFactory;

/** */
public class MetricsReportingExecutionListenerTest {

  @Mock Appender<ILoggingEvent> mockAppender;
  MetricsCollectingExecutionListener delegate;
  Level oldLevel;
  Appender<ILoggingEvent> stdout;
  Logger root;

  @Before
  public void prepareMocks() {
    MockitoAnnotations.initMocks(this);
    when(mockAppender.getName()).thenReturn("MOCK");
    Logger logger = (Logger) LoggerFactory.getLogger(MetricsReportingExecutionListener.class);
    logger.addAppender(mockAppender);
    oldLevel = logger.getLevel();
    logger.setLevel(Level.INFO);
    delegate = new MetricsCollectingExecutionListener();
    root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    stdout = root.getAppender("STDOUT");
    root.detachAppender(stdout);
  }

  @After
  public void restoreAppenders() {
    Logger logger = (Logger) LoggerFactory.getLogger(MetricsReportingExecutionListener.class);
    logger.detachAppender(mockAppender);
    logger.setLevel(oldLevel);
    root.addAppender(stdout);
  }

  @Test
  public void should_report_reads() throws Exception {
    MetricsReportingExecutionListener listener =
        MetricsReportingExecutionListener.builder()
            .reportingReads()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .build();

    listener.report();

    verifyEventLogged(
        "Reads: total: 0, successful: 0, failed: 0; 0 reads/second (mean 0.00, 75p 0.00, 99p 0.00 milliseconds)");

    // simulate 3 reads, 2 successful and 1 failed
    Timer total = delegate.getReadsTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulReadsCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedReadsCounter();
    failed.inc();

    listener.report();

    verifyEventLogged("Reads: total: 3, successful: 2, failed: 1");
    // cannot assert throughput in reads/second as it may vary
    verifyEventLogged("reads/second (mean 10.00, 75p 10.00, 99p 10.00 milliseconds)");
  }

  @Test
  public void should_report_reads_with_expected_total() throws Exception {
    MetricsReportingExecutionListener listener =
        MetricsReportingExecutionListener.builder()
            .reportingReads()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .expectingTotalEvents(3)
            .build();

    listener.report();

    verifyEventLogged(
        "Reads: total: 0, successful: 0, failed: 0; 0 reads/second, progression: 0% (mean 0.00, 75p 0.00, 99p 0.00 milliseconds)");

    // simulate 3 reads, 2 successful and 1 failed
    Timer total = delegate.getReadsTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulReadsCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedReadsCounter();
    failed.inc();

    listener.report();

    verifyEventLogged("Reads: total: 3, successful: 2, failed: 1");
    // cannot assert throughput in reads/second as it may vary
    verifyEventLogged(
        "reads/second, progression: 100% (mean 10.00, 75p 10.00, 99p 10.00 milliseconds)");
  }

  @Test
  public void should_report_writes() throws Exception {
    MetricsReportingExecutionListener listener =
        MetricsReportingExecutionListener.builder()
            .reportingWrites()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .build();

    listener.report();

    verifyEventLogged(
        "Writes: total: 0, successful: 0, failed: 0; 0 writes/second (mean 0.00, 75p 0.00, 99p 0.00 milliseconds)");

    // simulate 3 writes, 2 successful and 1 failed
    Timer total = delegate.getWritesTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulWritesCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedWritesCounter();
    failed.inc();

    listener.report();

    verifyEventLogged("Writes: total: 3, successful: 2, failed: 1");
    // cannot assert throughput in writes/second as it may vary
    verifyEventLogged("writes/second (mean 10.00, 75p 10.00, 99p 10.00 milliseconds)");
  }

  @Test
  public void should_report_writes_with_expected_total() throws Exception {
    MetricsReportingExecutionListener listener =
        MetricsReportingExecutionListener.builder()
            .reportingWrites()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .expectingTotalEvents(3)
            .build();

    listener.report();

    verifyEventLogged(
        "Writes: total: 0, successful: 0, failed: 0; 0 writes/second, progression: 0% (mean 0.00, 75p 0.00, 99p 0.00 milliseconds)");

    // simulate 3 writes, 2 successful and 1 failed
    Timer total = delegate.getWritesTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulWritesCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedWritesCounter();
    failed.inc();

    listener.report();

    verifyEventLogged("Writes: total: 3, successful: 2, failed: 1");
    // cannot assert throughput in writes/second as it may vary
    verifyEventLogged(
        "writes/second, progression: 100% (mean 10.00, 75p 10.00, 99p 10.00 milliseconds)");
  }

  @Test
  public void should_report_reads_and_writes() throws Exception {
    MetricsReportingExecutionListener listener =
        MetricsReportingExecutionListener.builder()
            .reportingReadsAndWrites()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .build();

    listener.report();

    verifyEventLogged(
        "Reads/Writes: total: 0, successful: 0, failed: 0; 0 reads-writes/second (mean 0.00, 75p 0.00, 99p 0.00 milliseconds)");

    // simulate 3 reads/writes, 2 successful and 1 failed
    Timer total = delegate.getReadsWritesTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulReadsWritesCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedReadsWritesCounter();
    failed.inc();

    listener.report();

    verifyEventLogged("Reads/Writes: total: 3, successful: 2, failed: 1");
    // cannot assert throughput in reads/writes/second as it may vary
    verifyEventLogged("reads-writes/second (mean 10.00, 75p 10.00, 99p 10.00 milliseconds)");
  }

  @Test
  public void should_report_reads_and_writes_with_default_constructor() throws Exception {
    MetricsReportingExecutionListener listener = new MetricsReportingExecutionListener();

    listener.report();

    verifyEventLogged(
        "Reads/Writes: total: 0, successful: 0, failed: 0; 0 reads-writes/second (mean 0.00, 75p 0.00, 99p 0.00 milliseconds)");

    MetricsCollectingExecutionListener delegate =
        (MetricsCollectingExecutionListener) Whitebox.getInternalState(listener, "delegate");
    // simulate 3 reads/writes, 2 successful and 1 failed
    Timer total = delegate.getReadsWritesTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulReadsWritesCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedReadsWritesCounter();
    failed.inc();

    listener.report();

    verifyEventLogged("Reads/Writes: total: 3, successful: 2, failed: 1");
    // cannot assert throughput in reads/writes/second as it may vary
    verifyEventLogged("reads-writes/second (mean 10.00, 75p 10.00, 99p 10.00 milliseconds)");
  }

  @Test
  public void should_report_reads_and_writes_with_expected_total() throws Exception {
    MetricsReportingExecutionListener listener =
        MetricsReportingExecutionListener.builder()
            .reportingReadsAndWrites()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .expectingTotalEvents(3)
            .build();

    listener.report();

    verifyEventLogged(
        "Reads/Writes: total: 0, successful: 0, failed: 0; 0 reads-writes/second, progression: 0% (mean 0.00, 75p 0.00, 99p 0.00 milliseconds)");

    // simulate 3 reads/writes, 2 successful and 1 failed
    Timer total = delegate.getReadsWritesTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulReadsWritesCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedReadsWritesCounter();
    failed.inc();

    listener.report();

    verifyEventLogged("Reads/Writes: total: 3, successful: 2, failed: 1");
    // cannot assert throughput in reads/writes/second as it may vary
    verifyEventLogged(
        "reads-writes/second, progression: 100% (mean 10.00, 75p 10.00, 99p 10.00 milliseconds)");
  }

  @Test
  public void should_report_statements() throws Exception {
    MetricsReportingExecutionListener listener =
        MetricsReportingExecutionListener.builder()
            .reportingStatements()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .build();

    listener.report();

    verifyEventLogged(
        "Statements: total: 0, successful: 0, failed: 0; 0 stmts/second (mean 0.00, 75p 0.00, 99p 0.00 milliseconds)");

    // simulate 3 stmts, 2 successful and 1 failed
    Timer total = delegate.getStatementsTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulStatementsCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedStatementsCounter();
    failed.inc();

    listener.report();

    verifyEventLogged("Statements: total: 3, successful: 2, failed: 1");
    // cannot assert throughput in stmts/second as it may vary
    verifyEventLogged("stmts/second (mean 10.00, 75p 10.00, 99p 10.00 milliseconds)");
  }

  @Test
  public void should_report_statements_with_expected_total() throws Exception {
    MetricsReportingExecutionListener listener =
        MetricsReportingExecutionListener.builder()
            .reportingStatements()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .expectingTotalEvents(3)
            .build();

    listener.report();

    verifyEventLogged(
        "Statements: total: 0, successful: 0, failed: 0; 0 stmts/second, progression: 0% (mean 0.00, 75p 0.00, 99p 0.00 milliseconds)");

    // simulate 3 stmts, 2 successful and 1 failed
    Timer total = delegate.getStatementsTimer();
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    total.update(10, MILLISECONDS);
    Counter successful = delegate.getSuccessfulStatementsCounter();
    successful.inc(2);
    Counter failed = delegate.getFailedStatementsCounter();
    failed.inc();

    listener.report();

    verifyEventLogged("Statements: total: 3, successful: 2, failed: 1");
    // cannot assert throughput in stmts/second as it may vary
    verifyEventLogged(
        "stmts/second, progression: 100% (mean 10.00, 75p 10.00, 99p 10.00 milliseconds)");
  }

  private void verifyEventLogged(String expectedLogMessage) {
    verify(mockAppender)
        .doAppend(
            argThat(
                new ArgumentMatcher<ILoggingEvent>() {
                  @Override
                  public boolean matches(final Object argument) {
                    return ((ILoggingEvent) argument)
                        .getFormattedMessage()
                        .contains(expectedLogMessage);
                  }
                }));
  }
}
