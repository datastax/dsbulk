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
import static org.mockito.Mockito.when;
import static org.slf4j.event.Level.DEBUG;

import com.datastax.dsbulk.commons.log.LogSink;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
@ExtendWith(LogInterceptingExtension.class)
class ReadsReportingExecutionListenerTest extends AbstractReportingExecutionListenerTest {

  ReadsReportingExecutionListenerTest(
      @LogCapture(value = ReadsReportingExecutionListener.class, level = DEBUG)
          LogInterceptor interceptor) {
    super(interceptor);
  }

  @BeforeEach
  void setUpCounters() {
    when(delegate.getTotalReadsTimer()).thenReturn(total);
    when(delegate.getSuccessfulReadsCounter()).thenReturn(successful);
    when(delegate.getFailedReadsCounter()).thenReturn(failed);
  }

  static Stream<Arguments> expectedMessages() {
    return Stream.of(
        Arguments.of(
            false,
            false,
            new String[] {
              "Reads: total: 100,000, successful: 99,999, failed: 1, in-flight: 500",
              "Throughput: 1,000 reads/second",
              "Latencies: mean 50.00, 75p 0.00, 99p 100.00, 999p 250.00 milliseconds"
            }),
        Arguments.of(
            false,
            true,
            new String[] {
              "Reads: total: 100,000, successful:  99,999, failed: 1, in-flight: 500, progression: 100%",
              "Throughput: 1,000 reads/second",
              "Latencies: mean 50.00, 75p 0.00, 99p 100.00, 999p 250.00 milliseconds"
            }),
        Arguments.of(
            true,
            false,
            new String[] {
              "Reads: total: 100,000, successful: 99,999, failed: 1, in-flight: 500",
              "Throughput: 1,000 reads/second, 1.00 mb/second (1.02 kb/read)",
              "Latencies: mean 50.00, 75p 0.00, 99p 100.00, 999p 250.00 milliseconds"
            }),
        Arguments.of(
            true,
            true,
            new String[] {
              "Reads: total: 100,000, successful:  99,999, failed: 1, in-flight: 500, progression: 100%",
              "Throughput: 1,000 reads/second, 1.00 mb/second (1.02 kb/read)",
              "Latencies: mean 50.00, 75p 0.00, 99p 100.00, 999p 250.00 milliseconds"
            }));
  }

  @ParameterizedTest(name = "[{index}] trackThroughput = {0} expectedTotal = {1}")
  @MethodSource("expectedMessages")
  void should_report_reads(
      boolean trackThroughput, boolean expectedTotal, String... expectedLines) {
    Logger logger = LoggerFactory.getLogger(ReadsReportingExecutionListener.class);

    if (trackThroughput) {
      when(delegate.getBytesReceivedMeter()).thenReturn(Optional.of(bytesReceived));
      when(bytesReceived.getMeanRate()).thenReturn(1024d * 1024d); // 1Mb per second
    }

    AbstractMetricsReportingExecutionListenerBuilder<ReadsReportingExecutionListener> builder =
        ReadsReportingExecutionListener.builder()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .withLogSink(LogSink.buildFrom(logger::isDebugEnabled, logger::debug));

    if (expectedTotal) {
      builder = builder.expectingTotalEvents(100_000);
    }

    ReadsReportingExecutionListener listener = builder.build();

    listener.report();

    for (String line : expectedLines) {
      assertThat(interceptor).hasMessageContaining(line);
    }
  }
}
