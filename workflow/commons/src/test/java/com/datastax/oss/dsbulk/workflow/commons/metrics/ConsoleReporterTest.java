/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.workflow.commons.metrics;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.datastax.oss.dsbulk.tests.logging.StreamCapture;
import com.datastax.oss.dsbulk.tests.logging.StreamInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.StreamInterceptor;
import com.datastax.oss.dsbulk.tests.logging.StreamType;
import com.datastax.oss.dsbulk.workflow.commons.settings.RowType;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.fusesource.jansi.Ansi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ConsoleReporterTest {

  @Mock private Timer writes;

  @Mock private Counter failed;

  @Mock private Meter bytes;

  @Mock private Histogram batches;

  @Mock private Snapshot latencies;

  @Mock private Snapshot batchSizes;

  private boolean ansiEnabled;

  @BeforeEach
  void disableAnsi() {
    ansiEnabled = Ansi.isEnabled();
    Ansi.setEnabled(false);
  }

  @AfterEach
  void reEnableAnsi() {
    Ansi.setEnabled(ansiEnabled);
  }

  static Stream<Arguments> arguments() {
    return Stream.of(
        Arguments.of(
            RowType.REGULAR,
            false,
            -1,
            false,
            ""
                + "  total | failed | rows/s | p50ms |  p99ms | p999ms"
                + System.lineSeparator()
                + "100,000 |      1 | 10,000 | 50.00 | 100.00 | 250.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.REGULAR,
            false,
            -1,
            true,
            ""
                + "  total | failed | rows/s | p50ms |  p99ms | p999ms | batches"
                + System.lineSeparator()
                + "100,000 |      1 | 10,000 | 50.00 | 100.00 | 250.00 |   32.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.REGULAR,
            false,
            100000,
            false,
            ""
                + "  total | failed | achieved | rows/s | p50ms |  p99ms | p999ms"
                + System.lineSeparator()
                + "100,000 |      1 |     100% | 10,000 | 50.00 | 100.00 | 250.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.REGULAR,
            false,
            100000,
            true,
            ""
                + "  total | failed | achieved | rows/s | p50ms |  p99ms | p999ms | batches"
                + System.lineSeparator()
                + "100,000 |      1 |     100% | 10,000 | 50.00 | 100.00 | 250.00 |   32.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.REGULAR,
            true,
            -1,
            false,
            ""
                + "  total | failed | rows/s | mb/s | kb/row | p50ms |  p99ms | p999ms"
                + System.lineSeparator()
                + "100,000 |      1 | 10,000 | 1.00 |   0.10 | 50.00 | 100.00 | 250.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.REGULAR,
            true,
            -1,
            true,
            ""
                + "  total | failed | rows/s | mb/s | kb/row | p50ms |  p99ms | p999ms | batches"
                + System.lineSeparator()
                + "100,000 |      1 | 10,000 | 1.00 |   0.10 | 50.00 | 100.00 | 250.00 |   32.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.REGULAR,
            true,
            100000,
            false,
            ""
                + "  total | failed | achieved | rows/s | mb/s | kb/row | p50ms |  p99ms | p999ms"
                + System.lineSeparator()
                + "100,000 |      1 |     100% | 10,000 | 1.00 |   0.10 | 50.00 | 100.00 | 250.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.REGULAR,
            true,
            100000,
            true,
            ""
                + "  total | failed | achieved | rows/s | mb/s | kb/row | p50ms |  p99ms | p999ms | batches"
                + System.lineSeparator()
                + "100,000 |      1 |     100% | 10,000 | 1.00 |   0.10 | 50.00 | 100.00 | 250.00 |   32.00"
                + System.lineSeparator()),
        // Vertices
        Arguments.of(
            RowType.VERTEX,
            false,
            -1,
            false,
            ""
                + "  total | failed | vertices/s | p50ms |  p99ms | p999ms"
                + System.lineSeparator()
                + "100,000 |      1 |     10,000 | 50.00 | 100.00 | 250.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.VERTEX,
            false,
            -1,
            true,
            ""
                + "  total | failed | vertices/s | p50ms |  p99ms | p999ms | batches"
                + System.lineSeparator()
                + "100,000 |      1 |     10,000 | 50.00 | 100.00 | 250.00 |   32.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.VERTEX,
            false,
            100000,
            false,
            ""
                + "  total | failed | achieved | vertices/s | p50ms |  p99ms | p999ms"
                + System.lineSeparator()
                + "100,000 |      1 |     100% |     10,000 | 50.00 | 100.00 | 250.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.VERTEX,
            false,
            100000,
            true,
            ""
                + "  total | failed | achieved | vertices/s | p50ms |  p99ms | p999ms | batches"
                + System.lineSeparator()
                + "100,000 |      1 |     100% |     10,000 | 50.00 | 100.00 | 250.00 |   32.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.VERTEX,
            true,
            -1,
            false,
            ""
                + "  total | failed | vertices/s | mb/s | kb/vertex | p50ms |  p99ms | p999ms"
                + System.lineSeparator()
                + "100,000 |      1 |     10,000 | 1.00 |      0.10 | 50.00 | 100.00 | 250.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.VERTEX,
            true,
            -1,
            true,
            ""
                + "  total | failed | vertices/s | mb/s | kb/vertex | p50ms |  p99ms | p999ms | batches"
                + System.lineSeparator()
                + "100,000 |      1 |     10,000 | 1.00 |      0.10 | 50.00 | 100.00 | 250.00 |   32.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.VERTEX,
            true,
            100000,
            false,
            ""
                + "  total | failed | achieved | vertices/s | mb/s | kb/vertex | p50ms |  p99ms | p999ms"
                + System.lineSeparator()
                + "100,000 |      1 |     100% |     10,000 | 1.00 |      0.10 | 50.00 | 100.00 | 250.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.VERTEX,
            true,
            100000,
            true,
            ""
                + "  total | failed | achieved | vertices/s | mb/s | kb/vertex | p50ms |  p99ms | p999ms | batches"
                + System.lineSeparator()
                + "100,000 |      1 |     100% |     10,000 | 1.00 |      0.10 | 50.00 | 100.00 | 250.00 |   32.00"
                + System.lineSeparator()),

        // Edges
        Arguments.of(
            RowType.EDGE,
            false,
            -1,
            false,
            ""
                + "  total | failed | edges/s | p50ms |  p99ms | p999ms"
                + System.lineSeparator()
                + "100,000 |      1 |  10,000 | 50.00 | 100.00 | 250.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.EDGE,
            false,
            -1,
            true,
            ""
                + "  total | failed | edges/s | p50ms |  p99ms | p999ms | batches"
                + System.lineSeparator()
                + "100,000 |      1 |  10,000 | 50.00 | 100.00 | 250.00 |   32.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.EDGE,
            false,
            100000,
            false,
            ""
                + "  total | failed | achieved | edges/s | p50ms |  p99ms | p999ms"
                + System.lineSeparator()
                + "100,000 |      1 |     100% |  10,000 | 50.00 | 100.00 | 250.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.EDGE,
            false,
            100000,
            true,
            ""
                + "  total | failed | achieved | edges/s | p50ms |  p99ms | p999ms | batches"
                + System.lineSeparator()
                + "100,000 |      1 |     100% |  10,000 | 50.00 | 100.00 | 250.00 |   32.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.EDGE,
            true,
            -1,
            false,
            ""
                + "  total | failed | edges/s | mb/s | kb/edge | p50ms |  p99ms | p999ms"
                + System.lineSeparator()
                + "100,000 |      1 |  10,000 | 1.00 |    0.10 | 50.00 | 100.00 | 250.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.EDGE,
            true,
            -1,
            true,
            ""
                + "  total | failed | edges/s | mb/s | kb/edge | p50ms |  p99ms | p999ms | batches"
                + System.lineSeparator()
                + "100,000 |      1 |  10,000 | 1.00 |    0.10 | 50.00 | 100.00 | 250.00 |   32.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.EDGE,
            true,
            100000,
            false,
            ""
                + "  total | failed | achieved | edges/s | mb/s | kb/edge | p50ms |  p99ms | p999ms"
                + System.lineSeparator()
                + "100,000 |      1 |     100% |  10,000 | 1.00 |    0.10 | 50.00 | 100.00 | 250.00"
                + System.lineSeparator()),
        Arguments.of(
            RowType.EDGE,
            true,
            100000,
            true,
            ""
                + "  total | failed | achieved | edges/s | mb/s | kb/edge | p50ms |  p99ms | p999ms | batches"
                + System.lineSeparator()
                + "100,000 |      1 |     100% |  10,000 | 1.00 |    0.10 | 50.00 | 100.00 | 250.00 |   32.00"
                + System.lineSeparator()));
  }

  @BeforeEach
  void setUpMocks() {
    when(writes.getCount()).thenReturn(100_000L); // 100,000 rows total
    when(writes.getMeanRate()).thenReturn(10_000d); // 10,000 rows/sec
    when(failed.getCount()).thenReturn(1L);
    when(writes.getSnapshot()).thenReturn(latencies);
    when(latencies.getMean()).thenReturn((double) MILLISECONDS.toNanos(50));
    when(latencies.get99thPercentile()).thenReturn((double) MILLISECONDS.toNanos(100));
    when(latencies.get999thPercentile()).thenReturn((double) MILLISECONDS.toNanos(250));
    when(bytes.getMeanRate()).thenReturn(1024d * 1024d); // 1Mb per second
    when(batches.getSnapshot()).thenReturn(batchSizes);
    when(batchSizes.getMean()).thenReturn(32d); // 32 stmts per batch in average
  }

  @ParameterizedTest(
      name = "[{index}] rowType = {0}, trackThroughput = {1} expectedTotal = {2} withBatches = {3}")
  @MethodSource("arguments")
  void should_report_on_console(
      RowType rowType,
      boolean trackThroughput,
      long expectedTotal,
      boolean withBatches,
      String expectedOutput,
      @StreamCapture(StreamType.STDERR) StreamInterceptor stderr) {

    // given
    ConsoleReporter reporter =
        new ConsoleReporter(
            new MetricRegistry(),
            new AtomicBoolean(true),
            writes::getCount,
            failed::getCount,
            writes,
            trackThroughput ? bytes : null,
            withBatches ? batches : null,
            SECONDS,
            MILLISECONDS,
            expectedTotal,
            new ScheduledThreadPoolExecutor(1),
            rowType);

    // when
    reporter.report();

    // then
    assertThat(stderr.getStreamAsString()).isEqualTo(expectedOutput);
  }
}
