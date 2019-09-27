/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log;

import static com.datastax.dsbulk.commons.internal.format.statement.StatementFormatVerbosity.EXTENDED;
import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.datastax.dsbulk.commons.tests.driver.DriverUtils.mockBoundStatement;
import static com.datastax.dsbulk.commons.tests.driver.DriverUtils.mockRow;
import static com.datastax.dsbulk.commons.tests.driver.DriverUtils.mockSession;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Fail.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.slf4j.event.Level.WARN;

import com.datastax.dsbulk.commons.internal.format.row.RowFormatter;
import com.datastax.dsbulk.commons.internal.format.statement.StatementFormatter;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.utils.FileUtils;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.internal.DefaultErrorRecord;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.format.statement.BulkBoundStatementPrinter;
import com.datastax.dsbulk.engine.internal.log.threshold.AbsoluteErrorThreshold;
import com.datastax.dsbulk.engine.internal.log.threshold.ErrorThreshold;
import com.datastax.dsbulk.engine.internal.log.threshold.RatioErrorThreshold;
import com.datastax.dsbulk.engine.internal.statement.BulkBoundStatement;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.result.DefaultReadResult;
import com.datastax.dsbulk.executor.api.internal.result.DefaultWriteResult;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverExecutionException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import reactor.core.publisher.Flux;

@ExtendWith(LogInterceptingExtension.class)
class LogManagerTest {

  private final String source1 = "line1\n";
  private final String source2 = "line2\n";
  private final String source3 = "line3\n";

  private URI resource1;
  private URI resource2;
  private URI resource3;

  private Record csvRecord1;
  private Record csvRecord2;
  private Record csvRecord3;

  private Record rowRecord1;
  private Record rowRecord2;
  private Record rowRecord3;

  private BatchableStatement<?> unmappableStmt1;
  private BatchableStatement<?> unmappableStmt2;
  private BatchableStatement<?> unmappableStmt3;

  private WriteResult failedWriteResult1;
  private WriteResult failedWriteResult2;
  private WriteResult failedWriteResult3;
  private WriteResult batchWriteResult;

  private ReadResult failedReadResult1;
  private ReadResult failedReadResult2;
  private ReadResult failedReadResult3;

  private Row row1;

  private ReadResult successfulReadResult1;

  private CqlSession session;

  private final StatementFormatter statementFormatter =
      StatementFormatter.builder()
          .withMaxQueryStringLength(500)
          .withMaxBoundValueLength(50)
          .withMaxBoundValues(10)
          .withMaxInnerStatements(10)
          .addStatementPrinters(new BulkBoundStatementPrinter())
          .build();

  private final RowFormatter rowFormatter = new RowFormatter();

  @BeforeEach
  void setUp() throws Exception {
    session = mockSession();
    resource1 = new URI("file:///file1.csv");
    resource2 = new URI("file:///file2.csv");
    resource3 = new URI("file:///file3.csv");
    csvRecord1 =
        new DefaultErrorRecord(source1, () -> resource1, 1, new RuntimeException("error 1"));
    csvRecord2 =
        new DefaultErrorRecord(source2, () -> resource2, 2, new RuntimeException("error 2"));
    csvRecord3 =
        new DefaultErrorRecord(source3, () -> resource3, 3, new RuntimeException("error 3"));
    unmappableStmt1 = new UnmappableStatement(csvRecord1, new RuntimeException("error 1"));
    unmappableStmt2 = new UnmappableStatement(csvRecord2, new RuntimeException("error 2"));
    unmappableStmt3 = new UnmappableStatement(csvRecord3, new RuntimeException("error 3"));
    failedWriteResult1 =
        new DefaultWriteResult(
            new BulkExecutionException(
                new DriverTimeoutException("error 1"),
                new BulkBoundStatement<>(csvRecord1, mockBoundStatement("INSERT 1"))));
    failedWriteResult2 =
        new DefaultWriteResult(
            new BulkExecutionException(
                new DriverTimeoutException("error 2"),
                new BulkBoundStatement<>(csvRecord2, mockBoundStatement("INSERT 2"))));
    failedWriteResult3 =
        new DefaultWriteResult(
            new BulkExecutionException(
                new DriverTimeoutException("error 3"),
                new BulkBoundStatement<>(csvRecord3, mockBoundStatement("INSERT 3"))));
    failedReadResult1 =
        new DefaultReadResult(
            new BulkExecutionException(
                new DriverTimeoutException("error 1"),
                new BulkBoundStatement<>(csvRecord1, mockBoundStatement("SELECT 1"))));
    failedReadResult2 =
        new DefaultReadResult(
            new BulkExecutionException(
                new DriverTimeoutException("error 2"),
                new BulkBoundStatement<>(csvRecord2, mockBoundStatement("SELECT 2"))));
    failedReadResult3 =
        new DefaultReadResult(
            new BulkExecutionException(
                new DriverTimeoutException("error 3"),
                new BulkBoundStatement<>(csvRecord3, mockBoundStatement("SELECT 3"))));
    BatchStatement batch =
        BatchStatement.newInstance(
            DefaultBatchType.UNLOGGED,
            new BulkBoundStatement<>(csvRecord1, mockBoundStatement("INSERT 1", "foo", 42)),
            new BulkBoundStatement<>(csvRecord2, mockBoundStatement("INSERT 2", "bar", 43)),
            new BulkBoundStatement<>(csvRecord3, mockBoundStatement("INSERT 3", "qix", 44)));
    batchWriteResult =
        new DefaultWriteResult(
            new BulkExecutionException(new DriverTimeoutException("error batch"), batch));
    ExecutionInfo info = mock(ExecutionInfo.class);
    row1 = mockRow(1);
    Row row2 = mockRow(2);
    Row row3 = mockRow(3);
    Statement<?> stmt1 = SimpleStatement.newInstance("SELECT 1");
    Statement<?> stmt2 = SimpleStatement.newInstance("SELECT 2");
    Statement<?> stmt3 = SimpleStatement.newInstance("SELECT 3");
    successfulReadResult1 = new DefaultReadResult(stmt1, info, row1);
    ReadResult successfulReadResult2 = new DefaultReadResult(stmt2, info, row2);
    ReadResult successfulReadResult3 = new DefaultReadResult(stmt3, info, row3);
    rowRecord1 =
        new DefaultErrorRecord(
            successfulReadResult1, () -> resource1, 1, new RuntimeException("error 1"));
    rowRecord2 =
        new DefaultErrorRecord(
            successfulReadResult2, () -> resource2, 2, new RuntimeException("error 2"));
    rowRecord3 =
        new DefaultErrorRecord(
            successfulReadResult3, () -> resource3, 3, new RuntimeException("error 3"));
  }

  @Test
  void should_stop_when_max_record_mapping_errors_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            WorkflowType.LOAD,
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(2),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    Flux<BatchableStatement<?>> stmts =
        Flux.just(unmappableStmt1, unmappableStmt2, unmappableStmt3);
    try {
      stmts.transform(logManager.newUnmappableStatementsHandler()).blockLast();
      fail("Expecting TooManyErrorsException to be thrown");
    } catch (TooManyErrorsException e) {
      assertThat(e).hasMessage("Too many errors, the maximum allowed is 2.");
      assertThat(((AbsoluteErrorThreshold) e.getThreshold()).getMaxErrors()).isEqualTo(2);
    }
    logManager.close();
    Path bad = logManager.getOperationDirectory().resolve("mapping.bad");
    Path errors = logManager.getOperationDirectory().resolve("mapping-errors.log");
    Path positions = logManager.getOperationDirectory().resolve("positions.txt");
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    assertThat(positions.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors, positions);
    List<String> badLines = Files.readAllLines(bad, UTF_8);
    assertThat(badLines).hasSize(3);
    assertThat(badLines.get(0)).isEqualTo(source1.trim());
    assertThat(badLines.get(1)).isEqualTo(source2.trim());
    assertThat(badLines.get(2)).isEqualTo(source3.trim());
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .containsOnlyOnce("Resource: " + resource1)
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source1))
        .containsOnlyOnce("java.lang.RuntimeException: error 1")
        .containsOnlyOnce("Resource: " + resource2)
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source2))
        .containsOnlyOnce("java.lang.RuntimeException: error 2")
        .containsOnlyOnce("Resource: " + resource3)
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source3))
        .containsOnlyOnce("java.lang.RuntimeException: error 3");
  }

  @Test
  void should_stop_at_first_error_when_max_errors_is_zero() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            WorkflowType.LOAD,
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(0),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    Flux<BatchableStatement<?>> stmts = Flux.just(unmappableStmt1);
    try {
      stmts.transform(logManager.newUnmappableStatementsHandler()).blockLast();
      fail("Expecting TooManyErrorsException to be thrown");
    } catch (TooManyErrorsException e) {
      assertThat(e).hasMessage("Too many errors, the maximum allowed is 0.");
      assertThat(((AbsoluteErrorThreshold) e.getThreshold()).getMaxErrors()).isEqualTo(0);
    }
    logManager.close();
    Path bad = logManager.getOperationDirectory().resolve("mapping.bad");
    Path errors = logManager.getOperationDirectory().resolve("mapping-errors.log");
    Path positions = logManager.getOperationDirectory().resolve("positions.txt");
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    assertThat(positions.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors, positions);
    List<String> badLines = Files.readAllLines(bad, UTF_8);
    assertThat(badLines).hasSize(1);
    assertThat(badLines.get(0)).isEqualTo(source1.trim());
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .containsOnlyOnce("Resource: " + resource1)
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source1))
        .containsOnlyOnce("java.lang.RuntimeException: error 1");
  }

  @Test
  void should_not_stop_when_max_errors_is_disabled() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            WorkflowType.LOAD,
            session,
            outputDir,
            ErrorThreshold.unlimited(),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    Flux<BatchableStatement<?>> stmts =
        Flux.just(unmappableStmt1, unmappableStmt2, unmappableStmt3);
    // should not throw TooManyErrorsException
    stmts.transform(logManager.newUnmappableStatementsHandler()).blockLast();
    logManager.close();
    Path bad = logManager.getOperationDirectory().resolve("mapping.bad");
    Path errors = logManager.getOperationDirectory().resolve("mapping-errors.log");
    Path positions = logManager.getOperationDirectory().resolve("positions.txt");
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    assertThat(positions.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors, positions);
    List<String> badLines = Files.readAllLines(bad, UTF_8);
    assertThat(badLines).hasSize(3);
    assertThat(badLines.get(0)).isEqualTo(source1.trim());
    assertThat(badLines.get(1)).isEqualTo(source2.trim());
    assertThat(badLines.get(2)).isEqualTo(source3.trim());
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .containsOnlyOnce("Resource: " + resource1)
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source1))
        .containsOnlyOnce("java.lang.RuntimeException: error 1")
        .containsOnlyOnce("Resource: " + resource2)
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source2))
        .containsOnlyOnce("java.lang.RuntimeException: error 2")
        .containsOnlyOnce("Resource: " + resource3)
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source3))
        .containsOnlyOnce("java.lang.RuntimeException: error 3");
  }

  @Test
  void should_stop_when_max_connector_errors_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            WorkflowType.LOAD,
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(2),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    Flux<Record> records = Flux.just(csvRecord1, csvRecord2, csvRecord3);
    try {
      records.transform(logManager.newFailedRecordsHandler()).blockLast();
      fail("Expecting TooManyErrorsException to be thrown");
    } catch (TooManyErrorsException e) {
      assertThat(e).hasMessage("Too many errors, the maximum allowed is 2.");
      assertThat(((AbsoluteErrorThreshold) e.getThreshold()).getMaxErrors()).isEqualTo(2);
    }
    logManager.close();
    Path bad = logManager.getOperationDirectory().resolve("connector.bad");
    Path errors = logManager.getOperationDirectory().resolve("connector-errors.log");
    Path positions = logManager.getOperationDirectory().resolve("positions.txt");
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    assertThat(positions.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors, positions);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .containsOnlyOnce("Resource: " + resource1)
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source1))
        .containsOnlyOnce("java.lang.RuntimeException: error 1")
        .containsOnlyOnce("Resource: " + resource2)
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source2))
        .containsOnlyOnce("java.lang.RuntimeException: error 2");
  }

  @Test
  void should_stop_when_max_write_errors_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            WorkflowType.LOAD,
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(2),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    Flux<WriteResult> stmts = Flux.just(failedWriteResult1, failedWriteResult2, failedWriteResult3);
    try {
      stmts.transform(logManager.newFailedWritesHandler()).blockLast();
      fail("Expecting TooManyErrorsException to be thrown");
    } catch (TooManyErrorsException e) {
      assertThat(e).hasMessage("Too many errors, the maximum allowed is 2.");
      assertThat(((AbsoluteErrorThreshold) e.getThreshold()).getMaxErrors()).isEqualTo(2);
    }
    logManager.close();
    Path bad = logManager.getOperationDirectory().resolve("load.bad");
    Path errors = logManager.getOperationDirectory().resolve("load-errors.log");
    Path positions = logManager.getOperationDirectory().resolve("positions.txt");
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    assertThat(positions.toFile()).exists();
    List<String> badLines = Files.readAllLines(bad, UTF_8);
    assertThat(badLines).hasSize(3);
    assertThat(badLines.get(0)).isEqualTo(source1.trim());
    assertThat(badLines.get(1)).isEqualTo(source2.trim());
    assertThat(badLines.get(2)).isEqualTo(source3.trim());
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors, positions);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .containsOnlyOnce("Resource: " + resource1)
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source1))
        .contains("INSERT 1")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 1 (error 1)")
        .containsOnlyOnce("Resource: " + resource2)
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source2))
        .contains("INSERT 2")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 2 (error 2)")
        .containsOnlyOnce("Resource: " + resource3)
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source3))
        .contains("INSERT 3")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 3 (error 3)");
    List<String> positionLines = Files.readAllLines(positions, UTF_8);
    assertThat(positionLines)
        .contains("file:///file1.csv:1")
        .contains("file:///file2.csv:2")
        .contains("file:///file3.csv:3");
  }

  @Test
  void should_not_stop_before_sample_size_is_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            WorkflowType.LOAD,
            session,
            outputDir,
            ErrorThreshold.forRatio(0.2f, 100),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    Flux<WriteResult> stmts = Flux.just(failedWriteResult1, failedWriteResult2, failedWriteResult3);
    stmts.transform(logManager.newFailedWritesHandler()).blockLast();
    logManager.close();
    Path bad = logManager.getOperationDirectory().resolve("load.bad");
    Path errors = logManager.getOperationDirectory().resolve("load-errors.log");
    Path positions = logManager.getOperationDirectory().resolve("positions.txt");
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    assertThat(positions.toFile()).exists();
    List<String> badLines = Files.readAllLines(bad, UTF_8);
    assertThat(badLines).hasSize(3);
    assertThat(badLines.get(0)).isEqualTo(source1.trim());
    assertThat(badLines.get(1)).isEqualTo(source2.trim());
    assertThat(badLines.get(2)).isEqualTo(source3.trim());
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors, positions);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .containsOnlyOnce("Resource: " + resource1)
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source1))
        .contains("INSERT 1")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 1 (error 1)")
        .containsOnlyOnce("Resource: " + resource2)
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source2))
        .contains("INSERT 2")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 2 (error 2)")
        .containsOnlyOnce("Resource: " + resource3)
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source3))
        .contains("INSERT 3")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 3 (error 3)");
    List<String> positionLines = Files.readAllLines(positions, UTF_8);
    assertThat(positionLines)
        .contains("file:///file1.csv:1")
        .contains("file:///file2.csv:2")
        .contains("file:///file3.csv:3");
  }

  @Test
  void should_stop_when_max_write_errors_reached_and_statements_batched() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            WorkflowType.LOAD,
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(1),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    Flux<WriteResult> stmts = Flux.just(batchWriteResult);
    try {
      stmts.transform(logManager.newFailedWritesHandler()).blockLast();
      fail("Expecting TooManyErrorsException to be thrown");
    } catch (TooManyErrorsException e) {
      assertThat(e).hasMessage("Too many errors, the maximum allowed is 1.");
      assertThat(((AbsoluteErrorThreshold) e.getThreshold()).getMaxErrors()).isEqualTo(1);
    }
    logManager.close();
    Path bad = logManager.getOperationDirectory().resolve("load.bad");
    Path errors = logManager.getOperationDirectory().resolve("load-errors.log");
    Path positions = logManager.getOperationDirectory().resolve("positions.txt");
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    assertThat(positions.toFile()).exists();
    List<String> badLines = Files.readAllLines(bad, UTF_8);
    assertThat(badLines).hasSize(3);
    assertThat(badLines.get(0)).isEqualTo(source1.trim());
    assertThat(badLines.get(1)).isEqualTo(source2.trim());
    assertThat(badLines.get(2)).isEqualTo(source3.trim());
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors, positions);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .containsOnlyOnce("Resource: " + resource1.toString())
        .containsOnlyOnce("Resource: " + resource2.toString())
        .containsOnlyOnce("Resource: " + resource3.toString())
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source1))
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source2))
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source3))
        .contains("INSERT 1")
        .contains("INSERT 2")
        .contains("INSERT 3")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed")
        .contains("error batch");
    List<String> positionLines = Files.readAllLines(positions, UTF_8);
    assertThat(positionLines)
        .contains("file:///file1.csv:1")
        .contains("file:///file2.csv:2")
        .contains("file:///file3.csv:3");
  }

  @Test
  void should_stop_when_max_read_errors_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            WorkflowType.UNLOAD,
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(2),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    Flux<ReadResult> stmts = Flux.just(failedReadResult1, failedReadResult2, failedReadResult3);
    try {
      stmts.transform(logManager.newFailedReadsHandler()).blockLast();
      fail("Expecting TooManyErrorsException to be thrown");
    } catch (TooManyErrorsException e) {
      assertThat(e).hasMessage("Too many errors, the maximum allowed is 2.");
      assertThat(((AbsoluteErrorThreshold) e.getThreshold()).getMaxErrors()).isEqualTo(2);
    }
    logManager.close();
    Path errors = logManager.getOperationDirectory().resolve("unload-errors.log");
    assertThat(errors.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(errors);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .contains("SELECT 1")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: SELECT 1 (error 1)")
        .contains("SELECT 2")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: SELECT 2 (error 2)");
  }

  @Test
  void should_stop_when_max_result_mapping_errors_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            WorkflowType.UNLOAD,
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(2),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    Flux<Record> stmts = Flux.just(rowRecord1, rowRecord2, rowRecord3);
    try {
      stmts.transform(logManager.newUnmappableRecordsHandler()).blockLast();
      fail("Expecting TooManyErrorsException to be thrown");
    } catch (TooManyErrorsException e) {
      assertThat(e).hasMessage("Too many errors, the maximum allowed is 2.");
      assertThat(((AbsoluteErrorThreshold) e.getThreshold()).getMaxErrors()).isEqualTo(2);
    }
    logManager.close();
    Path errors = logManager.getOperationDirectory().resolve("mapping-errors.log");
    assertThat(errors.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(errors);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .contains("SELECT 1")
        .containsOnlyOnce("c1: 1")
        .containsOnlyOnce("java.lang.RuntimeException: error 1")
        .contains("SELECT 2")
        .containsOnlyOnce("c1: 2")
        .containsOnlyOnce("java.lang.RuntimeException: error 2")
        .doesNotContain("c3: 3");
  }

  @Test
  void should_print_raw_bytes_when_column_cannot_be_properly_deserialized() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            WorkflowType.UNLOAD,
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(2),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    // Emulate bad row with corrupted data, see DefaultReadResultMapper
    IllegalArgumentException cause =
        new IllegalArgumentException("Invalid 32-bits integer value, expecting 4 bytes but got 5");
    IllegalArgumentException iae =
        new IllegalArgumentException(
            "Could not deserialize column c1 of type int as java.lang.Integer", cause);
    when(row1.getObject(0)).thenThrow(cause);
    when(row1.getBytesUnsafe(0)).thenReturn(ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5}));
    rowRecord1 = new DefaultErrorRecord(successfulReadResult1, () -> resource1, 1, iae);
    logManager.init();
    Flux<Record> stmts = Flux.just(rowRecord1);
    stmts.transform(logManager.newUnmappableRecordsHandler()).blockLast();
    logManager.close();
    Path errors = logManager.getOperationDirectory().resolve("mapping-errors.log");
    assertThat(errors.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(errors);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .contains("SELECT 1")
        .contains("c1: 0x0102030405 (malformed buffer for type INT)")
        .contains(iae.getMessage())
        .contains(cause.getMessage());
  }

  @Test
  void should_not_stop_when_sample_size_is_not_met() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            WorkflowType.UNLOAD,
            session,
            outputDir,
            ErrorThreshold.forRatio(0.01f, 100),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    Flux<ReadResult> stmts = Flux.just(failedReadResult1, failedReadResult2, failedReadResult3);
    stmts
        .transform(logManager.newTotalItemsCounter())
        .transform(logManager.newFailedReadsHandler())
        .blockLast();
    logManager.close();
    Path errors = logManager.getOperationDirectory().resolve("unload-errors.log");
    assertThat(errors.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(errors);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .contains("SELECT 1")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: SELECT 1 (error 1)")
        .contains("SELECT 2")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: SELECT 2 (error 2)");
  }

  @Test
  void should_stop_when_sample_size_is_met_and_percentage_exceeded() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            WorkflowType.UNLOAD,
            session,
            outputDir,
            ErrorThreshold.forRatio(0.01f, 100),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    Flux<ReadResult> stmts = Flux.just(failedReadResult1);
    try {
      stmts
          .repeat(101)
          .transform(logManager.newTotalItemsCounter())
          .transform(logManager.newFailedReadsHandler())
          .blockLast();
      fail("Expecting TooManyErrorsException to be thrown");
    } catch (TooManyErrorsException e) {
      assertThat(e).hasMessage("Too many errors, the maximum allowed is 1%.");
      assertThat(((RatioErrorThreshold) e.getThreshold()).getMaxErrorRatio()).isEqualTo(0.01f);
    }
    logManager.close();
    Path errors = logManager.getOperationDirectory().resolve("unload-errors.log");
    assertThat(errors.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(errors);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    assertThat(lines.stream().filter(l -> l.contains("BulkExecutionException")).count())
        .isEqualTo(100);
  }

  @Test
  void should_stop_when_unrecoverable_error_writing() throws Exception {
    Path outputDir = Files.createTempDirectory("test4");
    LogManager logManager =
        new LogManager(
            WorkflowType.LOAD,
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(1000),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    DefaultWriteResult result =
        new DefaultWriteResult(
            new BulkExecutionException(
                new DriverExecutionException(new IllegalArgumentException("error 1")),
                new BulkBoundStatement<>(csvRecord1, mockBoundStatement("INSERT 1"))));
    Flux<WriteResult> stmts = Flux.just(result);
    try {
      stmts.transform(logManager.newFailedWritesHandler()).blockLast();
      fail("Expecting DriverExecutionException to be thrown");
    } catch (DriverExecutionException e) {
      assertThat(e.getCause()).isInstanceOf(IllegalArgumentException.class).hasMessage("error 1");
    }
    logManager.close();
    Path bad = logManager.getOperationDirectory().resolve("load.bad");
    Path errors = logManager.getOperationDirectory().resolve("load-errors.log");
    Path positions = logManager.getOperationDirectory().resolve("positions.txt");
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    assertThat(positions.toFile()).exists();
    List<String> badLines = Files.readAllLines(bad, UTF_8);
    assertThat(badLines).hasSize(1);
    assertThat(badLines.get(0)).isEqualTo(source1.trim());
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors, positions);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .containsOnlyOnce("Resource: " + resource1)
        .containsOnlyOnce("Source: " + LogUtils.formatSingleLine(source1))
        .contains("INSERT 1")
        .contains("error 1")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 1");
    List<String> positionLines = Files.readAllLines(positions, UTF_8);
    assertThat(positionLines).contains("file:///file1.csv:1");
  }

  @Test
  void should_stop_when_unrecoverable_error_reading() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            WorkflowType.UNLOAD,
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(2),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    DefaultReadResult result =
        new DefaultReadResult(
            new BulkExecutionException(
                new DriverExecutionException(new IllegalArgumentException("error 1")),
                new BulkBoundStatement<>(csvRecord1, mockBoundStatement("SELECT 1"))));
    Flux<ReadResult> stmts = Flux.just(result);
    try {
      stmts.transform(logManager.newFailedReadsHandler()).blockLast();
      fail("Expecting DriverExecutionException to be thrown");
    } catch (DriverExecutionException e) {
      assertThat(e.getCause()).isInstanceOf(IllegalArgumentException.class).hasMessage("error 1");
    }
    logManager.close();
    Path errors = logManager.getOperationDirectory().resolve("unload-errors.log");
    assertThat(errors.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(errors);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .contains("SELECT 1")
        .contains("error 1")
        .containsOnlyOnce(
            "com.datastax.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: SELECT 1");
  }

  @Test
  void should_stop_when_max_cas_errors_reached() throws Exception {
    BatchStatement casBatch =
        BatchStatement.newInstance(
            DefaultBatchType.UNLOGGED,
            mockBulkBoundStatement(1, source1, resource1),
            mockBulkBoundStatement(2, source2, resource2),
            mockBulkBoundStatement(3, source3, resource3));
    Row row1 = mockRow(1);
    Row row2 = mockRow(2);
    Row row3 = mockRow(3);
    AsyncResultSet rs = mock(AsyncResultSet.class);
    when(rs.wasApplied()).thenReturn(false);
    when(rs.currentPage()).thenReturn(Lists.newArrayList(row1, row2, row3));
    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    when(rs.getExecutionInfo()).thenReturn(executionInfo);
    DefaultWriteResult casBatchWriteResult = new DefaultWriteResult(casBatch, rs);
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            WorkflowType.LOAD,
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(2),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    Flux<WriteResult> stmts = Flux.just(casBatchWriteResult);
    try {
      stmts.transform(logManager.newFailedWritesHandler()).blockLast();
      fail("Expecting TooManyErrorsException to be thrown");
    } catch (TooManyErrorsException e) {
      assertThat(e).hasMessage("Too many errors, the maximum allowed is 2.");
      assertThat(((AbsoluteErrorThreshold) e.getThreshold()).getMaxErrors()).isEqualTo(2);
    }
    logManager.close();
    Path bad = logManager.getOperationDirectory().resolve("paxos.bad");
    Path errors = logManager.getOperationDirectory().resolve("paxos-errors.log");
    Path positions = logManager.getOperationDirectory().resolve("positions.txt");
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    assertThat(positions.toFile()).exists();
    List<String> badLines = Files.readAllLines(bad, UTF_8);
    assertThat(badLines).hasSize(3);
    assertThat(badLines.get(0)).isEqualTo(source1.trim());
    assertThat(badLines.get(1)).isEqualTo(source2.trim());
    assertThat(badLines.get(2)).isEqualTo(source3.trim());
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors, positions);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .containsOnlyOnce("INSERT INTO 1")
        .contains("c1: 1")
        .containsOnlyOnce("INSERT INTO 2")
        .contains("c1: 2")
        .containsOnlyOnce("INSERT INTO 3")
        .contains("c1: 3");
  }

  @Test
  void should_log_query_warnings_when_reading(
      @LogCapture(value = LogManager.class, level = WARN) LogInterceptor logs) throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            WorkflowType.UNLOAD,
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(100),
            ErrorThreshold.forAbsoluteValue(1),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    ExecutionInfo info1 = mock(ExecutionInfo.class);
    when(info1.getWarnings()).thenReturn(ImmutableList.of("warning1", "warning2"));
    ExecutionInfo info2 = mock(ExecutionInfo.class);
    when(info2.getWarnings()).thenReturn(ImmutableList.of("warning3"));
    Flux.just(
            new DefaultReadResult(SimpleStatement.newInstance("SELECT 1"), info1, mockRow(1)),
            new DefaultReadResult(SimpleStatement.newInstance("SELECT 2"), info2, mockRow(2)))
        .transform(logManager.newQueryWarningsHandler())
        .blockLast();
    logManager.close();
    assertThat(logs)
        .hasMessageContaining("Query generated server-side warning: warning1")
        .doesNotHaveMessageContaining("warning2")
        .doesNotHaveMessageContaining("warning3")
        .hasMessageContaining(
            "The maximum number of logged query warnings has been exceeded (1); "
                + "subsequent warnings will not be logged.");
  }

  @Test
  void should_log_query_warnings_when_writing(
      @LogCapture(value = LogManager.class, level = WARN) LogInterceptor logs) throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            WorkflowType.UNLOAD,
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(100),
            ErrorThreshold.forAbsoluteValue(1),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    ExecutionInfo info1 = mock(ExecutionInfo.class);
    when(info1.getWarnings()).thenReturn(ImmutableList.of("warning1", "warning2"));
    AsyncResultSet rs1 = mock(AsyncResultSet.class);
    when(rs1.getExecutionInfo()).thenReturn(info1);
    ExecutionInfo info2 = mock(ExecutionInfo.class);
    when(info2.getWarnings()).thenReturn(ImmutableList.of("warning3"));
    AsyncResultSet rs2 = mock(AsyncResultSet.class);
    when(rs2.getExecutionInfo()).thenReturn(info2);
    Flux.just(
            new DefaultWriteResult(SimpleStatement.newInstance("SELECT 1"), rs1),
            new DefaultWriteResult(SimpleStatement.newInstance("SELECT 2"), rs2))
        .transform(logManager.newQueryWarningsHandler())
        .blockLast();
    logManager.close();
    assertThat(logs)
        .hasMessageContaining("Query generated server-side warning: warning1")
        .doesNotHaveMessageContaining("warning2")
        .doesNotHaveMessageContaining("warning3")
        .hasMessageContaining(
            "The maximum number of logged query warnings has been exceeded (1); "
                + "subsequent warnings will not be logged.");
  }

  private static BulkBoundStatement<?> mockBulkBoundStatement(
      int value, Object source, URI resource) {
    BoundStatement bs = mockBoundStatement("INSERT INTO " + value, value);
    return new BulkBoundStatement<>(DefaultRecord.indexed(source, resource, 1, 1), bs);
  }
}
