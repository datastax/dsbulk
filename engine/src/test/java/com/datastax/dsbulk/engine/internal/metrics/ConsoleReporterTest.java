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
import com.datastax.dsbulk.engine.internal.settings.RowType;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.fusesource.jansi.Ansi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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

  @ParameterizedTest
  @EnumSource(RowType.class)
  void should_report_on_console_without_expected_total_and_without_batches(
      RowType rowType, @StreamCapture(StreamType.STDERR) StreamInterceptor stderr) {
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
            new ScheduledThreadPoolExecutor(1),
            rowType);
    reporter.report();
    assertThat(stderr.getStreamAsString())
        .matches(
            "total \\| failed \\| "
                + rowType.plural()
                + "/s \\| mb/s \\| kb/"
                + rowType.singular()
                + " \\| p50ms \\| p99ms \\| p999ms"
                + System.lineSeparator()
                + "\\s+0 \\|\\s+0 \\|\\s+0 \\|\\s+0\\.00 \\|\\s+0\\.00 \\|\\s+0\\.00 \\|\\s+0\\.00 \\|\\s+0\\.00"
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
            "total \\| failed \\| "
                + rowType.plural()
                + "/s \\| mb/s \\| kb/"
                + rowType.singular()
                + " \\| p50ms \\| p99ms \\| p999ms"
                + System.lineSeparator()
                + "\\s+3 \\|\\s+1 \\|\\s+[\\d,.]+ \\|\\s+[\\d,.]+ \\|\\s+[\\d,.]+ \\|\\s+[\\d,.]+ \\|\\s+[\\d,.]+ \\|\\s+[\\d,.]+"
                + System.lineSeparator());
  }

  @ParameterizedTest
  @EnumSource(RowType.class)
  void should_report_on_console_with_expected_total_and_with_batches(
      RowType rowType, @StreamCapture(StreamType.STDERR) StreamInterceptor stderr) {
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
            new ScheduledThreadPoolExecutor(1),
            rowType);
    reporter.report();
    assertThat(stderr.getStreamAsString())
        .matches(
            "total \\| failed \\| achieved \\| "
                + rowType.plural()
                + "/s \\| mb/s \\| kb/"
                + rowType.singular()
                + " \\| p50ms \\| p99ms \\| p999ms \\| batches"
                + System.lineSeparator()
                + "\\s+0 \\|\\s+0 \\|\\s+0% \\|\\s+0 \\|\\s+0\\.00 \\|\\s+0\\.00 \\|\\s+0\\.00 \\|\\s+0\\.00 \\|\\s+0\\.00 \\|\\s+0\\.00"
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
            "total \\| failed \\| achieved \\| "
                + rowType.plural()
                + "/s \\| mb/s \\| kb/"
                + rowType.singular()
                + " \\| p50ms \\| p99ms \\| p999ms \\| batches"
                + System.lineSeparator()
                + "\\s+3 \\|\\s+1 \\|\\s+0% \\|\\s+[\\d,]+ \\|\\s+[\\d,.]+ \\|\\s+[\\d,.]+ \\|\\s+[\\d,.]+ \\|\\s+[\\d,.]+ \\|\\s+[\\d,.]+ \\|   15\\.00"
                + System.lineSeparator());
  }
}
