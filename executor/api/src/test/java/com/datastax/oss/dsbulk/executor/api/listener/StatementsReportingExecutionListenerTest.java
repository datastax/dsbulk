/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.executor.api.listener;

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.when;
import static org.slf4j.event.Level.DEBUG;

import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
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
class StatementsReportingExecutionListenerTest extends AbstractReportingExecutionListenerTest {

  StatementsReportingExecutionListenerTest(
      @LogCapture(value = StatementsReportingExecutionListener.class, level = DEBUG)
          LogInterceptor interceptor) {
    super(interceptor);
  }

  @BeforeEach
  void setUpCounters() {
    when(delegate.getTotalStatementsTimer()).thenReturn(total);
    when(delegate.getSuccessfulStatementsCounter()).thenReturn(successful);
    when(delegate.getFailedStatementsCounter()).thenReturn(failed);
  }

  static Stream<Arguments> expectedMessages() {
    return Stream.of(
        Arguments.of(
            false,
            false,
            new String[] {
              "Statements: total: 100,000, successful: 99,999, failed: 1, in-flight: 500",
              "Throughput: 1,000 stmts/second",
              "Latencies: mean 50.00, 75p 0.00, 99p 100.00, 999p 250.00 milliseconds"
            }),
        Arguments.of(
            false,
            true,
            new String[] {
              "Statements: total: 100,000, successful:  99,999, failed: 1, in-flight: 500, progression: 100%",
              "Throughput: 1,000 stmts/second",
              "Latencies: mean 50.00, 75p 0.00, 99p 100.00, 999p 250.00 milliseconds"
            }),
        Arguments.of(
            true,
            false,
            new String[] {
              "Statements: total: 100,000, successful: 99,999, failed: 1, in-flight: 500",
              "Throughput: 1,000 stmts/second, 1.00 mb/second sent, 1.00 mb/second received (1.02 kb/write, 1.02 kb/read)",
              "Latencies: mean 50.00, 75p 0.00, 99p 100.00, 999p 250.00 milliseconds"
            }),
        Arguments.of(
            true,
            true,
            new String[] {
              "Statements: total: 100,000, successful:  99,999, failed: 1, in-flight: 500, progression: 100%",
              "Throughput: 1,000 stmts/second, 1.00 mb/second sent, 1.00 mb/second received (1.02 kb/write, 1.02 kb/read)",
              "Latencies: mean 50.00, 75p 0.00, 99p 100.00, 999p 250.00 milliseconds"
            }));
  }

  @ParameterizedTest(name = "[{index}] trackThroughput = {0} expectedTotal = {1}")
  @MethodSource("expectedMessages")
  void should_report_statements(
      boolean trackThroughput, boolean expectedTotal, String... expectedLines) {
    Logger logger = LoggerFactory.getLogger(StatementsReportingExecutionListener.class);

    if (trackThroughput) {
      when(delegate.getBytesSentMeter()).thenReturn(Optional.of(bytesSent));
      when(delegate.getBytesReceivedMeter()).thenReturn(Optional.of(bytesReceived));
      when(bytesSent.getMeanRate()).thenReturn(1024d * 1024d); // 1Mb per second
      when(bytesReceived.getMeanRate()).thenReturn(1024d * 1024d); // 1Mb per second
    }

    AbstractMetricsReportingExecutionListenerBuilder<StatementsReportingExecutionListener> builder =
        StatementsReportingExecutionListener.builder()
            .convertDurationsTo(MILLISECONDS)
            .convertRatesTo(SECONDS)
            .extractingMetricsFrom(delegate)
            .withLogSink(LogSink.buildFrom(logger::isDebugEnabled, logger::debug));

    if (expectedTotal) {
      builder = builder.expectingTotalEvents(100_000);
    }

    StatementsReportingExecutionListener listener = builder.build();

    listener.report();

    for (String line : expectedLines) {
      assertThat(interceptor).hasMessageContaining(line);
    }
  }
}
