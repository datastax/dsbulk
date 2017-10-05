/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.log;

import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatVerbosity.EXTENDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.datastax.dsbulk.connectors.api.internal.DefaultUnmappableRecord;
import com.datastax.dsbulk.engine.internal.log.statement.StatementFormatter;
import com.datastax.dsbulk.engine.internal.statement.BulkSimpleStatement;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.result.DefaultReadResult;
import com.datastax.dsbulk.executor.api.internal.result.DefaultWriteResult;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import reactor.core.publisher.Flux;

/** */
@SuppressWarnings("FieldCanBeLocal")
public class LogManagerTest {

  private String source1 = "line1\n";
  private String source2 = "line2\n";
  private String source3 = "line3\n";

  private URI location1;
  private URI location2;
  private URI location3;

  private Record record1;
  private Record record2;
  private Record record3;

  private Statement stmt1;
  private Statement stmt2;
  private Statement stmt3;

  private WriteResult writeResult1;
  private WriteResult writeResult2;
  private WriteResult writeResult3;
  private WriteResult batchWriteResult;

  private ReadResult readResult1;
  private ReadResult readResult2;
  private ReadResult readResult3;

  private Cluster cluster;

  private ListeningExecutorService executor = MoreExecutors.newDirectExecutorService();

  private StatementFormatter formatter =
      StatementFormatter.builder()
          .withMaxQueryStringLength(500)
          .withMaxBoundValueLength(50)
          .withMaxBoundValues(10)
          .withMaxInnerStatements(10)
          .build();

  @Before
  public void setUp() throws Exception {
    cluster = mock(Cluster.class);
    Configuration configuration = mock(Configuration.class);
    ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
    when(cluster.getConfiguration()).thenReturn(configuration);
    when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
    when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);
    when(configuration.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT_INSTANCE);
    location1 = new URI("file:///file1.csv?line=1");
    location2 = new URI("file:///file2.csv?line=2");
    location3 = new URI("file:///file3.csv?line=3");
    record1 =
        new DefaultUnmappableRecord(source1, () -> this.location1, new RuntimeException("error 1"));
    record2 =
        new DefaultUnmappableRecord(source2, () -> this.location2, new RuntimeException("error 2"));
    record3 =
        new DefaultUnmappableRecord(source3, () -> this.location3, new RuntimeException("error 3"));
    stmt1 = new UnmappableStatement(record1, "f1", "c1", new RuntimeException("error 1"));
    stmt2 = new UnmappableStatement(record2, "f2", "c2", new RuntimeException("error 2"));
    stmt3 = new UnmappableStatement(record3, "f3", "c3", new RuntimeException("error 3"));
    writeResult1 =
        new DefaultWriteResult(
            new BulkExecutionException(
                new RuntimeException("error 1"), new BulkSimpleStatement<>(record1, "INSERT 1")));
    writeResult2 =
        new DefaultWriteResult(
            new BulkExecutionException(
                new RuntimeException("error 2"), new BulkSimpleStatement<>(record2, "INSERT 2")));
    writeResult3 =
        new DefaultWriteResult(
            new BulkExecutionException(
                new RuntimeException("error 3"), new BulkSimpleStatement<>(record3, "INSERT 3")));
    readResult1 =
        new DefaultReadResult(
            new BulkExecutionException(
                new RuntimeException("error 1"), new BulkSimpleStatement<>(record1, "SELECT 1")));
    readResult2 =
        new DefaultReadResult(
            new BulkExecutionException(
                new RuntimeException("error 2"), new BulkSimpleStatement<>(record2, "SELECT 2")));
    readResult3 =
        new DefaultReadResult(
            new BulkExecutionException(
                new RuntimeException("error 3"), new BulkSimpleStatement<>(record3, "SELECT 3")));
    BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
    batch.add(new BulkSimpleStatement<>(record1, "INSERT 1", "foo", 42));
    batch.add(new BulkSimpleStatement<>(record2, "INSERT 2", "bar", 43));
    batch.add(new BulkSimpleStatement<>(record3, "INSERT 3", "qix", 44));
    batchWriteResult =
        new DefaultWriteResult(
            new BulkExecutionException(new RuntimeException("error batch"), batch));
  }

  @Test
  public void should_stop_when_max_record_mapping_errors_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test2");
    LogManager logManager = new LogManager(cluster, outputDir, executor, 2, formatter, EXTENDED);
    logManager.init();
    Flux<Statement> stmts = Flux.just(stmt1, stmt2, stmt3);
    try {
      stmts.compose(logManager.newUnmappableStatementErrorHandler()).blockLast();
      fail("Expecting TooManyErrorsException to be thrown");
    } catch (TooManyErrorsException e) {
      assertThat(e).hasMessage("Too many errors, the maximum allowed is 2");
      assertThat(e.getMaxErrors()).isEqualTo(2);
    }
    logManager.close();
    Path bad = logManager.getExecutionDirectory().resolve("operation.bad");
    Path errors = logManager.getExecutionDirectory().resolve("record-mapping-errors.log");
    Path locations = logManager.getExecutionDirectory().resolve("locations.txt");
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    assertThat(locations.toFile()).exists();
    assertThat(Files.list(logManager.getExecutionDirectory()).toArray())
        .containsOnly(bad, errors, locations);
    List<String> badLines = Files.readAllLines(bad, Charset.forName("UTF-8"));
    assertThat(badLines).hasSize(3);
    assertThat(badLines.get(0)).isEqualTo(source1.trim());
    assertThat(badLines.get(1)).isEqualTo(source2.trim());
    assertThat(badLines.get(2)).isEqualTo(source3.trim());
    List<String> lines = Files.readAllLines(errors, Charset.forName("UTF-8"));
    String content = String.join("\n", lines);
    assertThat(content)
        .containsOnlyOnce("Location: " + location1)
        .containsOnlyOnce("Source  : " + LogUtils.formatSingleLine(source1))
        .containsOnlyOnce("java.lang.RuntimeException: error 1")
        .containsOnlyOnce("Location: " + location2)
        .containsOnlyOnce("Source  : " + LogUtils.formatSingleLine(source2))
        .containsOnlyOnce("java.lang.RuntimeException: error 2")
        .containsOnlyOnce("Location: " + location3)
        .containsOnlyOnce("Source  : " + LogUtils.formatSingleLine(source3))
        .containsOnlyOnce("java.lang.RuntimeException: error 2");
  }

  @Test
  public void should_stop_when_max_result_mapping_errors_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test1");
    LogManager logManager = new LogManager(cluster, outputDir, executor, 2, formatter, EXTENDED);
    logManager.init();
    Flux<Record> records = Flux.just(record1, record2, record3);
    try {
      records.compose(logManager.newUnmappableRecordErrorHandler()).blockLast();
      fail("Expecting TooManyErrorsException to be thrown");
    } catch (TooManyErrorsException e) {
      assertThat(e).hasMessage("Too many errors, the maximum allowed is 2");
      assertThat(e.getMaxErrors()).isEqualTo(2);
    }
    logManager.close();
    Path errors = logManager.getExecutionDirectory().resolve("result-mapping-errors.log");
    Path locations = logManager.getExecutionDirectory().resolve("locations.txt");
    assertThat(errors.toFile()).exists();
    assertThat(locations.toFile()).exists();
    assertThat(Files.list(logManager.getExecutionDirectory()).toArray())
        .containsOnly(errors, locations);
    List<String> lines = Files.readAllLines(errors, Charset.forName("UTF-8"));
    String content = String.join("\n", lines);
    assertThat(content)
        .containsOnlyOnce("Location: " + location1)
        .containsOnlyOnce("Source  : " + LogUtils.formatSingleLine(source1))
        .containsOnlyOnce("java.lang.RuntimeException: error 1")
        .containsOnlyOnce("Location: " + location2)
        .containsOnlyOnce("Source  : " + LogUtils.formatSingleLine(source2))
        .containsOnlyOnce("java.lang.RuntimeException: error 2");
  }

  @Test
  public void should_stop_when_max_write_errors_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test3");
    LogManager logManager = new LogManager(cluster, outputDir, executor, 2, formatter, EXTENDED);
    logManager.init();
    Flux<WriteResult> stmts = Flux.just(writeResult1, writeResult2, writeResult3);
    try {
      stmts.compose(logManager.newWriteErrorHandler()).blockLast();
      fail("Expecting TooManyErrorsException to be thrown");
    } catch (TooManyErrorsException e) {
      assertThat(e).hasMessage("Too many errors, the maximum allowed is 2");
      assertThat(e.getMaxErrors()).isEqualTo(2);
    }
    logManager.close();
    Path bad = logManager.getExecutionDirectory().resolve("operation.bad");
    Path errors = logManager.getExecutionDirectory().resolve("load-errors.log");
    Path locations = logManager.getExecutionDirectory().resolve("locations.txt");
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    assertThat(locations.toFile()).exists();
    List<String> badLines = Files.readAllLines(bad, Charset.forName("UTF-8"));
    assertThat(badLines).hasSize(3);
    assertThat(badLines.get(0)).isEqualTo(source1.trim());
    assertThat(badLines.get(1)).isEqualTo(source2.trim());
    assertThat(badLines.get(2)).isEqualTo(source3.trim());
    assertThat(Files.list(logManager.getExecutionDirectory()).toArray())
        .containsOnly(bad, errors, locations);
    List<String> lines = Files.readAllLines(errors, Charset.forName("UTF-8"));
    String content = String.join("\n", lines);
    assertThat(content)
        .containsOnlyOnce("Location: " + location1)
        .containsOnlyOnce("Source  : " + LogUtils.formatSingleLine(source1))
        .contains("INSERT 1")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 1 (error 1)")
        .containsOnlyOnce("Location: " + location2)
        .containsOnlyOnce("Source  : " + LogUtils.formatSingleLine(source2))
        .contains("INSERT 2")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 2 (error 2)")
        .containsOnlyOnce("Location: " + location3)
        .containsOnlyOnce("Source  : " + LogUtils.formatSingleLine(source3))
        .contains("INSERT 3")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 3 (error 3)");
  }

  @Test
  public void should_stop_when_max_write_errors_reached_and_statements_batched() throws Exception {
    Path outputDir = Files.createTempDirectory("test4");
    LogManager logManager = new LogManager(cluster, outputDir, executor, 1, formatter, EXTENDED);
    logManager.init();
    Flux<WriteResult> stmts = Flux.just(batchWriteResult, writeResult1);
    try {
      stmts.compose(logManager.newWriteErrorHandler()).blockLast();
      fail("Expecting TooManyErrorsException to be thrown");
    } catch (TooManyErrorsException e) {
      assertThat(e).hasMessage("Too many errors, the maximum allowed is 1");
      assertThat(e.getMaxErrors()).isEqualTo(1);
    }
    logManager.close();
    Path bad = logManager.getExecutionDirectory().resolve("operation.bad");
    Path errors = logManager.getExecutionDirectory().resolve("load-errors.log");
    Path locations = logManager.getExecutionDirectory().resolve("locations.txt");
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    assertThat(locations.toFile()).exists();
    List<String> badLines = Files.readAllLines(bad, Charset.forName("UTF-8"));
    assertThat(badLines).hasSize(3);
    assertThat(badLines.get(0)).isEqualTo(source1.trim());
    assertThat(badLines.get(1)).isEqualTo(source2.trim());
    assertThat(badLines.get(2)).isEqualTo(source3.trim());
    assertThat(Files.list(logManager.getExecutionDirectory()).toArray())
        .containsOnly(bad, errors, locations);
    List<String> lines = Files.readAllLines(errors, Charset.forName("UTF-8"));
    String content = String.join("\n", lines);
    assertThat(content)
        .containsOnlyOnce("Location: " + location1.toString())
        .containsOnlyOnce("Location: " + location2.toString())
        .containsOnlyOnce("Location: " + location3.toString())
        .containsOnlyOnce("Source  : " + LogUtils.formatSingleLine(source1))
        .containsOnlyOnce("Source  : " + LogUtils.formatSingleLine(source2))
        .containsOnlyOnce("Source  : " + LogUtils.formatSingleLine(source3))
        .contains("INSERT 1")
        .contains("INSERT 2")
        .contains("INSERT 3")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed")
        .contains("error batch");
  }

  @Test
  public void should_stop_when_max_read_errors_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test3");
    LogManager logManager = new LogManager(cluster, outputDir, executor, 2, formatter, EXTENDED);
    logManager.init();
    Flux<ReadResult> stmts = Flux.just(readResult1, readResult2, readResult3);
    try {
      stmts.compose(logManager.newReadErrorHandler()).blockLast();
      fail("Expecting TooManyErrorsException to be thrown");
    } catch (TooManyErrorsException e) {
      assertThat(e).hasMessage("Too many errors, the maximum allowed is 2");
      assertThat(e.getMaxErrors()).isEqualTo(2);
    }
    logManager.close();
    Path errors = logManager.getExecutionDirectory().resolve("unload-errors.log");
    Path locations = logManager.getExecutionDirectory().resolve("locations.txt");
    assertThat(errors.toFile()).exists();
    assertThat(locations.toFile()).exists();
    assertThat(Files.list(logManager.getExecutionDirectory()).toArray())
        .containsOnly(errors, locations);
    List<String> lines = Files.readAllLines(errors, Charset.forName("UTF-8"));
    String content = String.join("\n", lines);
    assertThat(content)
        .containsOnlyOnce("Location: " + location1)
        .containsOnlyOnce("Source  : " + LogUtils.formatSingleLine(source1))
        .contains("SELECT 1")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: SELECT 1 (error 1)")
        .containsOnlyOnce("Location: " + location2)
        .containsOnlyOnce("Source  : " + LogUtils.formatSingleLine(source2))
        .contains("SELECT 2")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: SELECT 2 (error 2)");
  }

  @Test
  public void should_record_locations() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager = new LogManager(cluster, outputDir, executor, 1, formatter, EXTENDED);
    logManager.init();
    @SuppressWarnings("unchecked")
    Cache<URI, Long> locations =
        (Cache<URI, Long>) Whitebox.getInternalState(logManager, "locations");
    Flux<WriteResult> stmts;
    stmts =
        Flux.just(result("file1", 1), result("file1", 2), result("file1", 3), result("file1", 4));
    stmts.compose(logManager.newLocationTracker()).blockLast();
    assertThat(locations.asMap())
        .hasSize(1)
        .hasEntrySatisfying(new URI("file1"), line -> assertThat(line).isEqualTo(4));
    stmts =
        Flux.just(result("file2", 1), result("file2", 2), result("file2", 3), result("file2", 5));
    stmts.compose(logManager.newLocationTracker()).blockLast();
    assertThat(locations.asMap())
        .hasSize(2)
        .hasEntrySatisfying(new URI("file1"), line -> assertThat(line).isEqualTo(4))
        .hasEntrySatisfying(new URI("file2"), line -> assertThat(line).isEqualTo(5));
    stmts =
        Flux.just(result("file3", 5), result("file3", 3), result("file3", 2), result("file3", 1));
    stmts.compose(logManager.newLocationTracker()).blockLast();
    assertThat(locations.asMap())
        .hasSize(3)
        .hasEntrySatisfying(new URI("file1"), line -> assertThat(line).isEqualTo(4))
        .hasEntrySatisfying(new URI("file2"), line -> assertThat(line).isEqualTo(5))
        .hasEntrySatisfying(new URI("file3"), line -> assertThat(line).isEqualTo(5));
    stmts =
        Flux.just(result("file4", 4), result("file4", 3), result("file4", 2), result("file4", 1));
    stmts.compose(logManager.newLocationTracker()).blockLast();
    assertThat(locations.asMap())
        .hasSize(4)
        .hasEntrySatisfying(new URI("file1"), line -> assertThat(line).isEqualTo(4))
        .hasEntrySatisfying(new URI("file2"), line -> assertThat(line).isEqualTo(5))
        .hasEntrySatisfying(new URI("file3"), line -> assertThat(line).isEqualTo(5))
        .hasEntrySatisfying(new URI("file4"), line -> assertThat(line).isEqualTo(4));
    stmts = Flux.just(result("file5", 4));
    stmts.compose(logManager.newLocationTracker()).blockLast();
    assertThat(locations.asMap())
        .hasSize(5)
        .hasEntrySatisfying(new URI("file1"), line -> assertThat(line).isEqualTo(4))
        .hasEntrySatisfying(new URI("file2"), line -> assertThat(line).isEqualTo(5))
        .hasEntrySatisfying(new URI("file3"), line -> assertThat(line).isEqualTo(5))
        .hasEntrySatisfying(new URI("file4"), line -> assertThat(line).isEqualTo(4))
        .hasEntrySatisfying(new URI("file5"), line -> assertThat(line).isEqualTo(4));
    logManager.close();
  }

  private WriteResult result(String file, int line) throws URISyntaxException {
    URI location = new URI(file + "?line=" + line);
    return new DefaultWriteResult(
        new BulkSimpleStatement<>(new DefaultRecord("irrelevant", () -> location), "INSERT 1"),
        null);
  }
}
