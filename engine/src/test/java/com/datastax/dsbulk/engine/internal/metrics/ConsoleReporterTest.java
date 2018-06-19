/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.metrics;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.dsbulk.commons.tests.logging.StreamCapture;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptor;
import com.datastax.dsbulk.commons.tests.logging.StreamType;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.fusesource.jansi.Ansi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(StreamInterceptingExtension.class)
class ConsoleReporterTest {

  private boolean ansiEnabled;

  @BeforeEach
  void setUp() {
    ansiEnabled = Ansi.isEnabled();
    Ansi.setEnabled(false);
  }

  @AfterEach
  void tearDown() {
    Ansi.setEnabled(ansiEnabled);
  }

  @Test
  void should_report_on_console_without_expected_total_and_without_batches(
      @StreamCapture(StreamType.STDERR) StreamInterceptor stderr) {
    MetricRegistry registry = new MetricRegistry();
    Timer writes = registry.timer("writes");
    Counter failed = registry.counter("failed");
    Meter bytes = registry.meter("bytes");
    Histogram batches = registry.histogram("batches");
    ConsoleReporter reporter =
        new ConsoleReporter(
            registry,
            new AtomicBoolean(true),
            writes::getCount,
            failed::getCount,
            writes,
            bytes,
            null,
            SECONDS,
            MILLISECONDS,
            -1,
            new ScheduledThreadPoolExecutor(1));
    reporter.report();
    assertThat(stderr.getStreamAsString())
        .isEqualTo(
            "total | failed | rows/s | mb/s | kb/row | p50ms | p99ms | p999ms"
                + System.lineSeparator()
                + "    0 |      0 |      0 | 0.00 |   0.00 |  0.00 |  0.00 |   0.00"
                + System.lineSeparator());
    stderr.clear();
    writes.update(10, MILLISECONDS);
    writes.update(10, MILLISECONDS);
    writes.update(10, MILLISECONDS);
    failed.inc();
    bytes.mark(10);
    bytes.mark(10);
    bytes.mark(10);
    batches.update(10);
    batches.update(20);
    reporter.report();
    assertThat(stderr.getStreamAsString())
        .matches(
            "total \\| failed \\| rows/s \\| mb/s \\| kb/row \\| p50ms \\| p99ms \\| p999ms"
                + System.lineSeparator()
                + "    3 \\|      1 \\|\\s+[\\d,.]+ \\|\\s+[\\d,.]+ \\|\\s+[\\d,.]+ \\|\\s+[\\d,.]+ \\|\\s+[\\d,.]+ \\|\\s+[\\d,.]+"
                + System.lineSeparator());
  }

  @Test
  void should_report_on_console_with_expected_total_and_with_batches(
      @StreamCapture(StreamType.STDERR) StreamInterceptor stderr) {
    MetricRegistry registry = new MetricRegistry();
    Timer writes = registry.timer("writes");
    Counter failed = registry.counter("failed");
    Meter bytes = registry.meter("bytes");
    Histogram batches = registry.histogram("batches");
    ConsoleReporter reporter =
        new ConsoleReporter(
            registry,
            new AtomicBoolean(true),
            writes::getCount,
            failed::getCount,
            writes,
            bytes,
            batches,
            SECONDS,
            MILLISECONDS,
            1000,
            new ScheduledThreadPoolExecutor(1));
    reporter.report();
    assertThat(stderr.getStreamAsString())
        .isEqualTo(
            "total | failed | achieved | rows/s | mb/s | kb/row | p50ms | p99ms | p999ms | batches"
                + System.lineSeparator()
                + "    0 |      0 |       0% |      0 | 0.00 |   0.00 |  0.00 |  0.00 |   0.00 |    0.00"
                + System.lineSeparator());
    stderr.clear();
    writes.update(10, MILLISECONDS);
    writes.update(10, MILLISECONDS);
    writes.update(10, MILLISECONDS);
    failed.inc();
    bytes.mark(10);
    bytes.mark(10);
    bytes.mark(10);
    batches.update(10);
    batches.update(20);
    reporter.report();
    assertThat(stderr.getStreamAsString())
        .matches(
            "total \\| failed \\| achieved \\| rows/s \\| mb/s \\| kb/row \\| p50ms \\| p99ms \\| p999ms \\| batches"
                + System.lineSeparator()
                + "    3 \\|      1 \\|       0% \\|\\s+[\\d,]+ \\|\\s+[\\d,.]+ \\|\\s+[\\d,.]+ \\|\\s+[\\d,.]+ \\|\\s+[\\d,.]+ \\|\\s+[\\d,.]+ \\|   15\\.00"
                + System.lineSeparator());
  }
}
