/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.metrics;

import static com.datastax.driver.core.ProtocolVersion.V4;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.internal.DefaultErrorRecord;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.statement.BulkSimpleStatement;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class MetricsManagerTest {

  private Record record1;
  private Record record2;
  private Record record3;

  private Statement stmt3;

  private BatchStatement batch;

  private ProtocolVersion protocolVersion = V4;
  private CodecRegistry codecRegistry = new CodecRegistry();

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
        new DefaultErrorRecord(
            source3, null, -1, () -> location3, new RuntimeException("irrelevant"));
    Statement stmt1 = new BulkSimpleStatement<>(record1, "irrelevant");
    Statement stmt2 = new BulkSimpleStatement<>(record2, "irrelevant");
    stmt3 =
        new UnmappableStatement(
            record3, () -> URI.create("http://record3"), new RuntimeException("irrelevant"));
    batch = new BatchStatement().add(stmt1).add(stmt2);
  }

  @Test
  void should_increment_records() throws Exception {
    MetricsManager manager =
        new MetricsManager(
            new MetricRegistry(),
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
            false,
            protocolVersion,
            codecRegistry);
    manager.init();
    Flux<Record> records = Flux.just(record1, record2, record3);
    records
        .transform(manager.newTotalItemsMonitor())
        .transform(manager.newFailedItemsMonitor())
        .blockLast();
    manager.close();
    MetricRegistry registry =
        (MetricRegistry) ReflectionUtils.getInternalState(manager, "registry");
    assertThat(registry.meter("records/total").getCount()).isEqualTo(3);
    assertThat(registry.counter("records/failed").getCount()).isEqualTo(1);
  }

  @Test
  void should_increment_batches() throws Exception {
    MetricsManager manager =
        new MetricsManager(
            new MetricRegistry(),
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
            true,
            protocolVersion,
            codecRegistry);
    manager.init();
    Flux<Statement> statements = Flux.just(batch, stmt3);
    statements.transform(manager.newBatcherMonitor()).blockLast();
    manager.close();
    MetricRegistry registry =
        (MetricRegistry) ReflectionUtils.getInternalState(manager, "registry");
    assertThat(registry.histogram("batches/size").getCount()).isEqualTo(2);
    assertThat(registry.histogram("batches/size").getSnapshot().getMean())
        .isEqualTo((2f + 1f) / 2f);
  }
}
