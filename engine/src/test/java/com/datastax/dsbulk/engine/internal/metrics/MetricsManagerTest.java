/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.metrics;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.datastax.dsbulk.connectors.api.internal.DefaultUnmappableRecord;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.statement.BulkSimpleStatement;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

class MetricsManagerTest {

  @Mock private Appender<ILoggingEvent> mockAppender;
  private Appender<ILoggingEvent> stdout;
  private Level oldLevel;
  private Logger root;

  private Record record1;
  private Record record2;
  private Record record3;

  private Statement stmt1;
  private Statement stmt2;
  private Statement stmt3;

  private BatchStatement batch;

  @BeforeEach
  void prepareMocks() {
    MockitoAnnotations.initMocks(this);
    when(mockAppender.getName()).thenReturn("MOCK");
    Logger logger = (Logger) LoggerFactory.getLogger("com.datastax.dsbulk.engine.internal.metrics");
    logger.addAppender(mockAppender);
    oldLevel = logger.getLevel();
    logger.setLevel(Level.INFO);
    root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    stdout = root.getAppender("STDOUT");
    root.detachAppender(stdout);
  }

  @AfterEach
  void restoreAppenders() {
    Logger logger = (Logger) LoggerFactory.getLogger("com.datastax.dsbulk.engine.internal.metrics");
    logger.detachAppender(mockAppender);
    logger.setLevel(oldLevel);
    root.addAppender(stdout);
  }

  @BeforeEach
  void setUp() throws Exception {
    URI location1 = new URI("file:///file1.csv?line=1");
    URI location2 = new URI("file:///file2.csv?line=2");
    URI location3 = new URI("file:///file3.csv?line=3");
    String source1 = "line1\n";
    String source2 = "line2\n";
    String source3 = "line3\n";
    record1 = new DefaultRecord(source1, null, -1, () -> location1, "irrelevant");
    record2 = new DefaultRecord(source2, null, -1, () -> location2, "irrelevant");
    record3 =
        new DefaultUnmappableRecord(
            source3, null, -1, () -> location3, new RuntimeException("irrelevant"));
    stmt1 = new BulkSimpleStatement<>(record1, "irrelevant");
    stmt2 = new BulkSimpleStatement<>(record2, "irrelevant");
    stmt3 =
        new UnmappableStatement(
            record3, () -> URI.create("http://record3"), new RuntimeException("irrelevant"));
    batch = new BatchStatement().add(stmt1).add(stmt2);
  }

  @Test
  void should_increment_records() throws Exception {
    MetricsManager manager =
        new MetricsManager(
            WorkflowType.UNLOAD,
            "test",
            Executors.newSingleThreadScheduledExecutor(),
            SECONDS,
            MILLISECONDS,
            -1,
            -1,
            false,
            false,
            null,
            Duration.ofSeconds(5),
            false);
    manager.init();
    Flux<Record> records = Flux.just(record1, record2, record3);
    records.transform(manager.newUnmappableRecordMonitor()).blockLast();
    manager.close();
    MetricRegistry registry = (MetricRegistry) Whitebox.getInternalState(manager, "registry");
    assertThat(registry.meter("records/total").getCount()).isEqualTo(3);
    assertThat(registry.meter("records/successful").getCount()).isEqualTo(2);
    assertThat(registry.meter("records/failed").getCount()).isEqualTo(1);
  }

  @Test
  void should_increment_mappings() throws Exception {
    MetricsManager manager =
        new MetricsManager(
            WorkflowType.LOAD,
            "test",
            Executors.newSingleThreadScheduledExecutor(),
            SECONDS,
            MILLISECONDS,
            -1,
            -1,
            false,
            false,
            null,
            Duration.ofSeconds(5),
            true);
    manager.init();
    Flux<Statement> statements = Flux.just(stmt1, stmt2, stmt3);
    statements.transform(manager.newUnmappableStatementMonitor()).blockLast();
    manager.close();
    MetricRegistry registry = (MetricRegistry) Whitebox.getInternalState(manager, "registry");
    assertThat(registry.meter("mappings/total").getCount()).isEqualTo(3);
    assertThat(registry.meter("mappings/successful").getCount()).isEqualTo(2);
    assertThat(registry.meter("mappings/failed").getCount()).isEqualTo(1);
  }

  @Test
  void should_increment_batches() throws Exception {
    MetricsManager manager =
        new MetricsManager(
            WorkflowType.LOAD,
            "test",
            Executors.newSingleThreadScheduledExecutor(),
            SECONDS,
            MILLISECONDS,
            -1,
            -1,
            false,
            false,
            null,
            Duration.ofSeconds(5),
            true);
    manager.init();
    Flux<Statement> statements = Flux.just(batch, stmt3);
    statements.transform(manager.newBatcherMonitor()).blockLast();
    manager.close();
    MetricRegistry registry = (MetricRegistry) Whitebox.getInternalState(manager, "registry");
    assertThat(registry.histogram("batches/size").getCount()).isEqualTo(2);
    assertThat(registry.histogram("batches/size").getSnapshot().getMean())
        .isEqualTo((2f + 1f) / 2f);
  }
}
