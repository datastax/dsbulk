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
package com.datastax.oss.dsbulk.workflow.commons.metrics;

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static com.datastax.oss.dsbulk.tests.logging.StreamType.STDERR;
import static com.datastax.oss.dsbulk.tests.utils.FileUtils.readFile;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.slf4j.event.Level.DEBUG;
import static org.slf4j.event.Level.INFO;
import static org.slf4j.event.Level.WARN;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.dsbulk.connectors.api.DefaultErrorRecord;
import com.datastax.oss.dsbulk.connectors.api.DefaultRecord;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.executor.api.listener.WritesReportingExecutionListener;
import com.datastax.oss.dsbulk.executor.api.result.Result;
import com.datastax.oss.dsbulk.executor.api.result.WriteResult;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogConfigurationResource;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.logging.StreamCapture;
import com.datastax.oss.dsbulk.tests.logging.StreamInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.StreamInterceptor;
import com.datastax.oss.dsbulk.tests.utils.ReflectionUtils;
import com.datastax.oss.dsbulk.workflow.commons.metrics.prometheus.PrometheusManager;
import com.datastax.oss.dsbulk.workflow.commons.settings.LogSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.RowType;
import com.datastax.oss.dsbulk.workflow.commons.statement.MappedSimpleStatement;
import com.datastax.oss.dsbulk.workflow.commons.statement.UnmappableStatement;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import reactor.core.publisher.Flux;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@LogConfigurationResource("logback.xml")
class MetricsManagerTest {

  private Record record1;
  private Record record2;
  private Record record3;

  @Mock private WriteResult result1;
  @Mock private WriteResult result2;
  @Mock private WriteResult result3;

  @Mock private BatchableStatement<?> stmt1;
  @Mock private BatchableStatement<?> stmt2;
  private BatchableStatement<?> stmt3;

  private BatchStatement batch;

  private final ProtocolVersion protocolVersion = ProtocolVersion.DEFAULT;
  private final CodecRegistry codecRegistry = CodecRegistry.DEFAULT;

  @BeforeEach
  void setUp() throws Exception {
    URI resource1 = new URI("file:///file1.csv");
    URI resource2 = new URI("file:///file2.csv");
    URI resource3 = new URI("file:///file3.csv");
    String source1 = "line1\n";
    String source2 = "line2\n";
    String source3 = "line3\n";
    record1 = DefaultRecord.indexed(source1, resource1, -1, "irrelevant");
    record2 = DefaultRecord.indexed(source2, resource2, -1, "irrelevant");
    record3 = new DefaultErrorRecord(source3, resource3, -1, new RuntimeException("irrelevant"));
    BatchableStatement<?> stmt1 =
        new MappedSimpleStatement(record1, SimpleStatement.newInstance("irrelevant"));
    BatchableStatement<?> stmt2 =
        new MappedSimpleStatement(record2, SimpleStatement.newInstance("irrelevant"));
    stmt3 = new UnmappableStatement(record3, new RuntimeException("irrelevant"));
    batch = BatchStatement.newInstance(DefaultBatchType.UNLOGGED).add(stmt1).add(stmt2);
  }

  @Test
  void should_increment_records(
      @LogCapture(value = MetricsManager.class, level = INFO) LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stderr) {
    try (MetricsManager manager =
        new MetricsManager(
            new MetricRegistry(),
            false,
            "test",
            Executors.newSingleThreadScheduledExecutor(),
            SECONDS,
            MILLISECONDS,
            -1,
            -1,
            true,
            false,
            false,
            true,
            null,
            null,
            LogSettings.Verbosity.normal,
            Duration.ofSeconds(5),
            false,
            protocolVersion,
            codecRegistry,
            RowType.REGULAR)) {
      manager.init(100, 10);
      manager.start();
      Flux<Record> records = Flux.just(record1, record2, record3);
      records
          .transform(manager.newTotalItemsMonitor())
          .transform(manager.newFailedRecordsMonitor())
          .blockLast();
      manager.stop(Duration.ofSeconds(123), true);
      MetricRegistry registry =
          (MetricRegistry) ReflectionUtils.getInternalState(manager, "registry");
      assertThat(registry.counter("records/total").getCount()).isEqualTo(103);
      assertThat(registry.counter("records/failed").getCount()).isEqualTo(11);
      assertThat(logs.getLoggedEvents()).isEmpty();
      assertThat(stderr.getStreamLinesPlain())
          .anySatisfy(line -> assertThat(line).startsWith("  103 |     11 |"));
    }
  }

  @Test
  void should_increment_mapped_statements(
      @LogCapture(value = MetricsManager.class, level = INFO) LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stderr) {
    try (MetricsManager manager =
        new MetricsManager(
            new MetricRegistry(),
            false,
            "test",
            Executors.newSingleThreadScheduledExecutor(),
            SECONDS,
            MILLISECONDS,
            -1,
            -1,
            true,
            false,
            false,
            true,
            null,
            null,
            LogSettings.Verbosity.normal,
            Duration.ofSeconds(5),
            false,
            protocolVersion,
            codecRegistry,
            RowType.REGULAR)) {
      manager.init(100, 10);
      manager.start();
      Flux<BatchableStatement<?>> records = Flux.just(stmt1, stmt2, stmt3);
      records
          .transform(manager.newTotalItemsMonitor())
          .transform(manager.newUnmappableStatementsMonitor())
          .blockLast();
      manager.stop(Duration.ofSeconds(123), true);
      MetricRegistry registry =
          (MetricRegistry) ReflectionUtils.getInternalState(manager, "registry");
      assertThat(registry.counter("records/total").getCount()).isEqualTo(103);
      assertThat(registry.counter("records/failed").getCount()).isEqualTo(11);
      assertThat(logs.getLoggedEvents()).isEmpty();
      assertThat(stderr.getStreamLinesPlain())
          .anySatisfy(line -> assertThat(line).startsWith("  103 |     11 |"));
    }
  }

  @Test
  void should_increment_results(
      @LogCapture(value = MetricsManager.class, level = INFO) LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stderr) {
    when(result1.isSuccess()).thenReturn(true);
    when(result2.isSuccess()).thenReturn(true);
    when(result3.isSuccess()).thenReturn(false);
    try (MetricsManager manager =
        new MetricsManager(
            new MetricRegistry(),
            false,
            "test",
            Executors.newSingleThreadScheduledExecutor(),
            SECONDS,
            MILLISECONDS,
            -1,
            -1,
            true,
            false,
            false,
            true,
            null,
            null,
            LogSettings.Verbosity.normal,
            Duration.ofSeconds(5),
            false,
            protocolVersion,
            codecRegistry,
            RowType.REGULAR)) {
      manager.init(100, 10);
      manager.start();
      Flux<Result> records = Flux.just(result1, result2, result3);
      records
          .transform(manager.newTotalItemsMonitor())
          .transform(manager.newFailedResultsMonitor())
          .blockLast();
      manager.stop(Duration.ofSeconds(123), true);
      MetricRegistry registry =
          (MetricRegistry) ReflectionUtils.getInternalState(manager, "registry");
      assertThat(registry.counter("records/total").getCount()).isEqualTo(103);
      assertThat(registry.counter("records/failed").getCount()).isEqualTo(11);
      assertThat(logs.getLoggedEvents()).isEmpty();
      assertThat(stderr.getStreamLinesPlain())
          .anySatisfy(line -> assertThat(line).startsWith("  103 |     11 |"));
    }
  }

  @Test
  void should_increment_batches(
      @LogCapture(value = MetricsManager.class, level = INFO) LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stderr) {
    try (MetricsManager manager =
        new MetricsManager(
            new MetricRegistry(),
            true,
            "test",
            Executors.newSingleThreadScheduledExecutor(),
            SECONDS,
            MILLISECONDS,
            -1,
            -1,
            true,
            false,
            false,
            true,
            null,
            null,
            LogSettings.Verbosity.normal,
            Duration.ofSeconds(5),
            true,
            protocolVersion,
            codecRegistry,
            RowType.REGULAR)) {
      manager.init(0, 0);
      manager.start();
      Flux<Statement<?>> statements = Flux.just(batch, stmt3);
      statements.transform(manager.newBatcherMonitor()).blockLast();
      manager.stop(Duration.ofSeconds(123), true);
      MetricRegistry registry =
          (MetricRegistry) ReflectionUtils.getInternalState(manager, "registry");
      assertThat(registry.histogram("batches").getCount()).isEqualTo(2);
      assertThat(registry.histogram("batches").getSnapshot().getMean()).isEqualTo((2f + 1f) / 2f);
      assertThat(logs.getLoggedEvents()).isEmpty();
      assertThat(stderr.getStreamLinesPlain())
          .anySatisfy(line -> assertThat(line).startsWith("    0 |      0 |"));
    }
  }

  @Test
  void should_log_final_stats_to_main_log_file_in_normal_mode(
      @LogCapture(value = MetricsManager.class, level = INFO) LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stderr)
      throws Exception {
    Path executionDirectory = Files.createTempDirectory("test");
    Path mainLogFile = executionDirectory.resolve("operation.log");
    LogSettings.createMainLogFileAppender(mainLogFile);
    MetricsManager manager =
        new MetricsManager(
            new MetricRegistry(),
            true,
            "test",
            Executors.newSingleThreadScheduledExecutor(),
            SECONDS,
            MILLISECONDS,
            -1,
            -1,
            true,
            false,
            false,
            true,
            null,
            executionDirectory,
            LogSettings.Verbosity.normal,
            Duration.ofSeconds(5),
            true,
            protocolVersion,
            codecRegistry,
            RowType.REGULAR);
    try {
      manager.init(0, 0);
      manager.start();
      WritesReportingExecutionListener writesReporter =
          (WritesReportingExecutionListener)
              ReflectionUtils.getInternalState(manager, "writesReporter");
      writesReporter.report();
      assertThat(logs.getLoggedMessages()).isEmpty();
    } finally {
      manager.stop(Duration.ofSeconds(123), true);
      manager.close();
    }
    manager.reportFinalMetrics();
    assertThat(logs)
        .hasMessageContaining("Writes:")
        .hasMessageContaining("Throughput:")
        .hasMessageContaining("Latencies:");
    assertThat(readFile(mainLogFile))
        .contains("Final stats:")
        .contains("Writes:")
        .contains("Throughput:")
        .contains("Latencies:");
    assertThat(stderr.getStreamAsString())
        .contains("total | failed | rows/s | mb/s | kb/row | p50ms | p99ms | p999ms | batches");
  }

  @Test
  void should_log_final_stats_to_main_log_file_in_quiet_mode(
      @LogCapture(value = MetricsManager.class, level = WARN) LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stderr)
      throws Exception {
    Path executionDirectory = Files.createTempDirectory("test");
    Path mainLogFile = executionDirectory.resolve("operation.log");
    LogSettings.createMainLogFileAppender(mainLogFile);
    LogSettings.setVerbosityQuiet();
    MetricsManager manager =
        new MetricsManager(
            new MetricRegistry(),
            true,
            "test",
            Executors.newSingleThreadScheduledExecutor(),
            SECONDS,
            MILLISECONDS,
            -1,
            -1,
            true,
            false,
            false,
            false,
            null,
            executionDirectory,
            LogSettings.Verbosity.quiet,
            Duration.ofSeconds(5),
            true,
            protocolVersion,
            codecRegistry,
            RowType.REGULAR);
    try {
      manager.init(0, 0);
      manager.start();
      WritesReportingExecutionListener writesReporter =
          (WritesReportingExecutionListener)
              ReflectionUtils.getInternalState(manager, "writesReporter");
      assertThat(writesReporter).isNull();
    } finally {
      manager.stop(Duration.ofSeconds(123), true);
      manager.close();
    }
    manager.reportFinalMetrics();
    assertThat(readFile(mainLogFile)).doesNotContain("Final stats:");
    assertThat(logs.getLoggedMessages()).isEmpty();
    assertThat(stderr.getStreamAsString()).isEmpty();
  }

  @Test
  void should_disable_console_reporter(@StreamCapture(STDERR) StreamInterceptor stderr)
      throws Exception {
    Path executionDirectory = Files.createTempDirectory("test");
    Path mainLogFile = executionDirectory.resolve("operation.log");
    LogSettings.createMainLogFileAppender(mainLogFile);
    LogSettings.setVerbosityHigh();
    MetricsManager manager =
        new MetricsManager(
            new MetricRegistry(),
            true,
            "test",
            Executors.newSingleThreadScheduledExecutor(),
            SECONDS,
            MILLISECONDS,
            -1,
            -1,
            true,
            false,
            false,
            false,
            null,
            executionDirectory,
            LogSettings.Verbosity.high,
            Duration.ofSeconds(5),
            true,
            protocolVersion,
            codecRegistry,
            RowType.REGULAR);
    try {
      manager.init(0, 0);
      manager.start();
      ConsoleReporter consoleReporter =
          (ConsoleReporter) ReflectionUtils.getInternalState(manager, "consoleReporter");
      assertThat(consoleReporter).isNull();
    } finally {
      manager.stop(Duration.ofSeconds(123), true);
      manager.close();
    }
    manager.reportFinalMetrics();
    assertThat(stderr.getStreamAsString()).isEmpty();
  }

  @Test
  void should_log_periodic_stats_to_main_log_file_in_verbose_mode(
      @LogCapture(value = MetricsManager.class, level = DEBUG) LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stderr)
      throws Exception {
    Path executionDirectory = Files.createTempDirectory("test");
    Path mainLogFile = executionDirectory.resolve("operation.log");
    LogSettings.createMainLogFileAppender(mainLogFile);
    LogSettings.setVerbosityHigh();
    MetricsManager manager =
        new MetricsManager(
            new MetricRegistry(),
            true,
            "test",
            Executors.newSingleThreadScheduledExecutor(),
            SECONDS,
            MILLISECONDS,
            -1,
            -1,
            true,
            false,
            false,
            true,
            null,
            executionDirectory,
            LogSettings.Verbosity.high,
            Duration.ofSeconds(5),
            true,
            protocolVersion,
            codecRegistry,
            RowType.REGULAR);
    try {
      manager.init(0, 0);
      manager.start();
      WritesReportingExecutionListener writesReporter =
          (WritesReportingExecutionListener)
              ReflectionUtils.getInternalState(manager, "writesReporter");
      writesReporter.report();
      assertThat(logs)
          .hasMessageContaining("Writes:")
          .hasMessageContaining("Throughput:")
          .hasMessageContaining("Latencies:");
    } finally {
      manager.stop(Duration.ofSeconds(123), true);
      manager.close();
    }
    manager.reportFinalMetrics();
    assertThat(logs)
        .hasMessageContaining("Writes:")
        .hasMessageContaining("Throughput:")
        .hasMessageContaining("Latencies:");
    assertThat(readFile(mainLogFile))
        .contains("Final stats:")
        .contains("Writes:")
        .contains("Throughput:")
        .contains("Latencies:");
    assertThat(stderr.getStreamAsString())
        .contains("total | failed | rows/s | mb/s | kb/row | p50ms | p99ms | p999ms | batches");
  }

  @Test
  void should_exchange_metrics_with_prometheus() throws Exception {
    Path executionDirectory = Files.createTempDirectory("test");
    PrometheusManager prometheus = mock(PrometheusManager.class);
    MetricsManager manager =
        new MetricsManager(
            new MetricRegistry(),
            true,
            "test",
            Executors.newSingleThreadScheduledExecutor(),
            SECONDS,
            MILLISECONDS,
            -1,
            -1,
            true,
            false,
            false,
            false,
            prometheus,
            executionDirectory,
            LogSettings.Verbosity.high,
            Duration.ofSeconds(5),
            true,
            protocolVersion,
            codecRegistry,
            RowType.REGULAR);
    manager.init(0, 0);
    manager.start();
    manager.stop(Duration.ofSeconds(123), true);
    manager.close();
    manager.reportFinalMetrics();
    verify(prometheus).init();
    verify(prometheus).start();
    verify(prometheus).close();
    verify(prometheus).pushMetrics(Duration.ofSeconds(123), true);
  }
}
