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
package com.datastax.oss.dsbulk.workflow.commons.log;

import static com.datastax.oss.dsbulk.format.statement.StatementFormatVerbosity.EXTENDED;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static com.datastax.oss.dsbulk.tests.driver.DriverUtils.mockBoundStatement;
import static com.datastax.oss.dsbulk.tests.driver.DriverUtils.mockRow;
import static com.datastax.oss.dsbulk.tests.driver.DriverUtils.mockSession;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Fail.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.slf4j.event.Level.WARN;

import com.datastax.oss.driver.api.core.CqlIdentifier;
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
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenRange;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.dsbulk.connectors.api.DefaultErrorRecord;
import com.datastax.oss.dsbulk.connectors.api.DefaultRecord;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.oss.dsbulk.executor.api.listener.DefaultExecutionContext;
import com.datastax.oss.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.oss.dsbulk.executor.api.result.DefaultReadResult;
import com.datastax.oss.dsbulk.executor.api.result.DefaultWriteResult;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.dsbulk.executor.api.result.WriteResult;
import com.datastax.oss.dsbulk.format.row.RowFormatter;
import com.datastax.oss.dsbulk.format.statement.StatementFormatter;
import com.datastax.oss.dsbulk.partitioner.utils.TokenUtils;
import com.datastax.oss.dsbulk.tests.driver.MockAsyncResultSet;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.utils.FileUtils;
import com.datastax.oss.dsbulk.workflow.api.error.AbsoluteErrorThreshold;
import com.datastax.oss.dsbulk.workflow.api.error.ErrorThreshold;
import com.datastax.oss.dsbulk.workflow.api.error.RatioErrorThreshold;
import com.datastax.oss.dsbulk.workflow.api.error.TooManyErrorsException;
import com.datastax.oss.dsbulk.workflow.commons.statement.MappedBoundStatement;
import com.datastax.oss.dsbulk.workflow.commons.statement.MappedBoundStatementPrinter;
import com.datastax.oss.dsbulk.workflow.commons.statement.RangeReadBoundStatement;
import com.datastax.oss.dsbulk.workflow.commons.statement.RangeReadBoundStatementPrinter;
import com.datastax.oss.dsbulk.workflow.commons.statement.RangeReadStatement;
import com.datastax.oss.dsbulk.workflow.commons.statement.UnmappableStatement;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.BiFunction;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

@ExtendWith(LogInterceptingExtension.class)
class LogManagerTest {

  private final URI tableResource1 = URI.create("cql://ks1/table1?start=1&end=2");
  private final URI tableResource2 = URI.create("cql://ks1/table1?start=2&end=3");
  private final URI tableResource3 = URI.create("cql://ks1/table1?start=3&end=4");

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
          .addStatementPrinters(
              new MappedBoundStatementPrinter(), new RangeReadBoundStatementPrinter())
          .build();

  private final RowFormatter rowFormatter = new RowFormatter();

  @BeforeEach
  void setUp() throws Exception {
    session = mockSession();
    resource1 = new URI("file:///file1.csv");
    resource2 = new URI("file:///file2.csv");
    resource3 = new URI("file:///file3.csv");
    csvRecord1 = new DefaultErrorRecord(source1, resource1, 1, new RuntimeException("error 1"));
    csvRecord2 = new DefaultErrorRecord(source2, resource2, 2, new RuntimeException("error 2"));
    csvRecord3 = new DefaultErrorRecord(source3, resource3, 3, new RuntimeException("error 3"));
    unmappableStmt1 = new UnmappableStatement(csvRecord1, new RuntimeException("error 1"));
    unmappableStmt2 = new UnmappableStatement(csvRecord2, new RuntimeException("error 2"));
    unmappableStmt3 = new UnmappableStatement(csvRecord3, new RuntimeException("error 3"));
    failedWriteResult1 =
        new DefaultWriteResult(
            new BulkExecutionException(
                new DriverTimeoutException("error 1"),
                new MappedBoundStatement(csvRecord1, mockBoundStatement("INSERT 1"))));
    failedWriteResult2 =
        new DefaultWriteResult(
            new BulkExecutionException(
                new DriverTimeoutException("error 2"),
                new MappedBoundStatement(csvRecord2, mockBoundStatement("INSERT 2"))));
    failedWriteResult3 =
        new DefaultWriteResult(
            new BulkExecutionException(
                new DriverTimeoutException("error 3"),
                new MappedBoundStatement(csvRecord3, mockBoundStatement("INSERT 3"))));
    TokenRange tokenRange1 = new Murmur3TokenRange(new Murmur3Token(1), new Murmur3Token(2));
    TokenRange tokenRange2 = new Murmur3TokenRange(new Murmur3Token(2), new Murmur3Token(3));
    TokenRange tokenRange3 = new Murmur3TokenRange(new Murmur3Token(3), new Murmur3Token(4));
    failedReadResult1 =
        new DefaultReadResult(
            new BulkExecutionException(
                new DriverTimeoutException("error 1"),
                new RangeReadBoundStatement(
                    mockBoundStatement("SELECT 1"),
                    tokenRange1,
                    URI.create(
                        String.format(
                            "cql://ks/t?start=%s&end=%s",
                            tokenRange1.getStart(), tokenRange1.getEnd())))));
    failedReadResult2 =
        new DefaultReadResult(
            new BulkExecutionException(
                new DriverTimeoutException("error 2"),
                new RangeReadBoundStatement(
                    mockBoundStatement("SELECT 2"),
                    tokenRange2,
                    URI.create(
                        String.format(
                            "cql://ks/t?start=%s&end=%s",
                            tokenRange2.getStart(), tokenRange2.getEnd())))));
    failedReadResult3 =
        new DefaultReadResult(
            new BulkExecutionException(
                new DriverTimeoutException("error 3"),
                new RangeReadBoundStatement(
                    mockBoundStatement("SELECT 3"),
                    tokenRange3,
                    URI.create(
                        String.format(
                            "cql://ks/t?start=%s&end=%s",
                            tokenRange3.getStart(), tokenRange3.getEnd())))));
    BatchStatement batch =
        BatchStatement.newInstance(
            DefaultBatchType.UNLOGGED,
            new MappedBoundStatement(csvRecord1, mockBoundStatement("INSERT 1", "foo", 42)),
            new MappedBoundStatement(csvRecord2, mockBoundStatement("INSERT 2", "bar", 43)),
            new MappedBoundStatement(csvRecord3, mockBoundStatement("INSERT 3", "qix", 44)));
    batchWriteResult =
        new DefaultWriteResult(
            new BulkExecutionException(new DriverTimeoutException("error batch"), batch));
    ExecutionInfo info = mock(ExecutionInfo.class);
    row1 = mockRow(1);
    Row row2 = mockRow(2);
    Row row3 = mockRow(3);
    Statement<?> stmt1 =
        new RangeReadBoundStatement(
            mockBoundStatement("SELECT 1"),
            tokenRange1,
            URI.create(
                String.format(
                    "cql://ks/t?start=%s&end=%s", tokenRange1.getStart(), tokenRange1.getEnd())));
    Statement<?> stmt2 =
        new RangeReadBoundStatement(
            mockBoundStatement("SELECT 2"),
            tokenRange2,
            URI.create(
                String.format(
                    "cql://ks/t?start=%s&end=%s", tokenRange2.getStart(), tokenRange2.getEnd())));
    Statement<?> stmt3 =
        new RangeReadBoundStatement(
            mockBoundStatement("SELECT 3"),
            tokenRange3,
            URI.create(
                String.format(
                    "cql://ks/t?start=%s&end=%s", tokenRange3.getStart(), tokenRange3.getEnd())));
    successfulReadResult1 = new DefaultReadResult(stmt1, info, row1, 1);
    ReadResult successfulReadResult2 = new DefaultReadResult(stmt2, info, row2, 2);
    ReadResult successfulReadResult3 = new DefaultReadResult(stmt3, info, row3, 3);
    rowRecord1 =
        new DefaultErrorRecord(
            successfulReadResult1, tableResource1, 1, new RuntimeException("error 1"));
    rowRecord2 =
        new DefaultErrorRecord(
            successfulReadResult2, tableResource2, 2, new RuntimeException("error 2"));
    rowRecord3 =
        new DefaultErrorRecord(
            successfulReadResult3, tableResource3, 3, new RuntimeException("error 3"));
  }

  @Test
  void should_stop_when_max_record_mapping_errors_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
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
      Assertions.assertThat(((AbsoluteErrorThreshold) e.getThreshold()).getMaxErrors())
          .isEqualTo(2);
    }
    logManager.close();
    Path bad = logManager.getOperationDirectory().resolve("mapping.bad");
    Path errors = logManager.getOperationDirectory().resolve("mapping-errors.log");
    Path positions = logManager.getOperationDirectory().resolve("summary.csv");
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
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source1))
        .containsOnlyOnce("java.lang.RuntimeException: error 1")
        .containsOnlyOnce("Resource: " + resource2)
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source2))
        .containsOnlyOnce("java.lang.RuntimeException: error 2")
        .containsOnlyOnce("Resource: " + resource3)
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source3))
        .containsOnlyOnce("java.lang.RuntimeException: error 3");
    List<String> positionLines = Files.readAllLines(positions, UTF_8);
    assertThat(positionLines)
        .contains("file:///file1.csv;[1,1];0")
        .contains("file:///file2.csv;[2,2];0")
        .contains("file:///file3.csv;[3,3];0");
  }

  @Test
  void should_stop_at_first_error_when_max_errors_is_zero() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
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
    Path positions = logManager.getOperationDirectory().resolve("summary.csv");
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
        .containsOnlyOnce("Position: 1")
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source1))
        .containsOnlyOnce("java.lang.RuntimeException: error 1");
  }

  @Test
  void should_not_stop_when_max_errors_is_disabled() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
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
    Path positions = logManager.getOperationDirectory().resolve("summary.csv");
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
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source1))
        .containsOnlyOnce("java.lang.RuntimeException: error 1")
        .containsOnlyOnce("Resource: " + resource2)
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source2))
        .containsOnlyOnce("java.lang.RuntimeException: error 2")
        .containsOnlyOnce("Resource: " + resource3)
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source3))
        .containsOnlyOnce("java.lang.RuntimeException: error 3");
  }

  @Test
  void should_stop_when_max_connector_errors_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
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
    Path positions = logManager.getOperationDirectory().resolve("summary.csv");
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    assertThat(positions.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors, positions);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .containsOnlyOnce("Resource: " + resource1)
        .containsOnlyOnce("Position: 1")
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source1))
        .containsOnlyOnce("java.lang.RuntimeException: error 1")
        .containsOnlyOnce("Resource: " + resource2)
        .containsOnlyOnce("Position: 2")
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source2))
        .containsOnlyOnce("java.lang.RuntimeException: error 2")
        .containsOnlyOnce("Resource: " + resource3)
        .containsOnlyOnce("Position: 3")
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source3))
        .containsOnlyOnce("java.lang.RuntimeException: error 3");
  }

  @Test
  void should_stop_when_max_write_errors_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
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
    Path positions = logManager.getOperationDirectory().resolve("summary.csv");
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
        .containsOnlyOnce("Position: 1")
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source1))
        .contains("INSERT 1")
        .containsOnlyOnce(
            "com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 1 (error 1)")
        .containsOnlyOnce("Resource: " + resource2)
        .containsOnlyOnce("Position: 2")
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source2))
        .contains("INSERT 2")
        .containsOnlyOnce(
            "com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 2 (error 2)")
        .containsOnlyOnce("Resource: " + resource3)
        .containsOnlyOnce("Position: 3")
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source3))
        .contains("INSERT 3")
        .containsOnlyOnce(
            "com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 3 (error 3)");
    List<String> positionLines = Files.readAllLines(positions, UTF_8);
    assertThat(positionLines)
        .contains("file:///file1.csv;[1,1];0")
        .contains("file:///file2.csv;[2,2];0")
        .contains("file:///file3.csv;[3,3];0");
  }

  @Test
  void should_not_stop_before_sample_size_is_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
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
    Path positions = logManager.getOperationDirectory().resolve("summary.csv");
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
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source1))
        .contains("INSERT 1")
        .containsOnlyOnce(
            "com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 1 (error 1)")
        .containsOnlyOnce("Resource: " + resource2)
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source2))
        .contains("INSERT 2")
        .containsOnlyOnce(
            "com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 2 (error 2)")
        .containsOnlyOnce("Resource: " + resource3)
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source3))
        .contains("INSERT 3")
        .containsOnlyOnce(
            "com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 3 (error 3)");
    List<String> positionLines = Files.readAllLines(positions, UTF_8);
    assertThat(positionLines)
        .contains("file:///file1.csv;[1,1];0")
        .contains("file:///file2.csv;[2,2];0")
        .contains("file:///file3.csv;[3,3];0");
  }

  @Test
  void should_stop_when_max_write_errors_reached_and_statements_batched() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
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
    Path positions = logManager.getOperationDirectory().resolve("summary.csv");
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
        .containsOnlyOnce("Position: 1")
        .containsOnlyOnce("Position: 2")
        .containsOnlyOnce("Position: 3")
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source1))
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source2))
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source3))
        .contains("INSERT 1")
        .contains("INSERT 2")
        .contains("INSERT 3")
        .containsOnlyOnce(
            "com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed")
        .contains("error batch");
    List<String> positionLines = Files.readAllLines(positions, UTF_8);
    assertThat(positionLines)
        .contains("file:///file1.csv;[1,1];0")
        .contains("file:///file2.csv;[2,2];0")
        .contains("file:///file3.csv;[3,3];0");
  }

  @Test
  void should_stop_when_max_read_errors_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
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
        .doesNotContain("Resource: ")
        .doesNotContain("Position: ")
        .doesNotContain("Source: ")
        .contains("SELECT 1")
        .containsOnlyOnce(
            "com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: SELECT 1 (error 1)")
        .contains("SELECT 2")
        .containsOnlyOnce(
            "com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: SELECT 2 (error 2)");
  }

  @Test
  void should_stop_when_max_result_mapping_errors_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
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
    Path bad = logManager.getOperationDirectory().resolve("mapping.bad");
    assertThat(errors.toFile()).exists();
    Path summary = logManager.getOperationDirectory().resolve("summary.csv");
    assertThat(errors.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(errors, bad, summary);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .doesNotContain("Source: ")
        .containsOnlyOnce("Resource: cql://ks1/table1?start=1&end=2")
        .containsOnlyOnce("Position: 1")
        .containsOnlyOnce("SELECT 1")
        .containsOnlyOnce("c1: 1")
        .containsOnlyOnce("java.lang.RuntimeException: error 1")
        .containsOnlyOnce("Resource: cql://ks1/table1?start=2&end=3")
        .containsOnlyOnce("Position: 2")
        .containsOnlyOnce("SELECT 2")
        .containsOnlyOnce("c1: 2")
        .containsOnlyOnce("java.lang.RuntimeException: error 2")
        .containsOnlyOnce("Resource: cql://ks1/table1?start=3&end=4")
        .containsOnlyOnce("Position: 3")
        .containsOnlyOnce("c1: 3")
        .containsOnlyOnce("java.lang.RuntimeException: error 3");
    assertThat(Files.readAllLines(bad, UTF_8)).containsExactly("[1]", "[2]", "[3]");
    assertThat(Files.readAllLines(summary, UTF_8))
        .containsExactly(
            "resource;ranges;done",
            "cql://ks1/table1?start=1&end=2;[1,1];0",
            "cql://ks1/table1?start=2&end=3;[2,2];0",
            "cql://ks1/table1?start=3&end=4;[3,3];0");
  }

  @Test
  void should_print_raw_bytes_when_column_cannot_be_properly_deserialized() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
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
    rowRecord1 = new DefaultErrorRecord(successfulReadResult1, tableResource1, 1, iae);
    logManager.init();
    Flux<Record> stmts = Flux.just(rowRecord1);
    stmts.transform(logManager.newUnmappableRecordsHandler()).blockLast();
    logManager.close();
    Path errors = logManager.getOperationDirectory().resolve("mapping-errors.log");
    assertThat(errors.toFile()).exists();
    Path bad = logManager.getOperationDirectory().resolve("mapping.bad");
    assertThat(errors.toFile()).exists();
    Path summary = logManager.getOperationDirectory().resolve("summary.csv");
    assertThat(errors.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(errors, bad, summary);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .contains("Resource: cql://ks1/table1?start=1&end=2")
        .contains("Position: 1")
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
        .doesNotContain("Resource: ")
        .doesNotContain("Source: ")
        .contains("SELECT 1")
        .containsOnlyOnce(
            "com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: SELECT 1 (error 1)")
        .contains("SELECT 2")
        .containsOnlyOnce(
            "com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: SELECT 2 (error 2)");
  }

  @Test
  void should_stop_when_sample_size_is_met_and_percentage_exceeded() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
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
      Assertions.assertThat(((RatioErrorThreshold) e.getThreshold()).getMaxErrorRatio())
          .isEqualTo(0.01f);
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
                new MappedBoundStatement(csvRecord1, mockBoundStatement("INSERT 1"))));
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
    Path positions = logManager.getOperationDirectory().resolve("summary.csv");
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
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source1))
        .contains("Position: 1")
        .contains("INSERT 1")
        .contains("error 1")
        .containsOnlyOnce(
            "com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 1");
    List<String> positionLines = Files.readAllLines(positions, UTF_8);
    assertThat(positionLines).contains("file:///file1.csv;[1,1];0");
  }

  @Test
  void should_stop_when_unrecoverable_error_reading() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
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
                mockBoundStatement("SELECT 1")));
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
        .doesNotContain("Resource: ")
        .doesNotContain("Source: ")
        .doesNotContain("Position: ")
        .contains("SELECT 1")
        .contains("error 1")
        .containsOnlyOnce(
            "com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException: Statement execution failed: SELECT 1");
  }

  @Test
  void should_stop_when_max_cas_errors_reached() throws Exception {
    BatchStatement casBatch =
        BatchStatement.newInstance(
            DefaultBatchType.UNLOGGED,
            mockMappedBoundStatement(1, source1, resource1),
            mockMappedBoundStatement(2, source2, resource2),
            mockMappedBoundStatement(3, source3, resource3));
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
    Path positions = logManager.getOperationDirectory().resolve("summary.csv");
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
        .containsOnlyOnce("Failed conditional updates: ")
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
            new DefaultReadResult(SimpleStatement.newInstance("SELECT 1"), info1, mockRow(1), 1),
            new DefaultReadResult(SimpleStatement.newInstance("SELECT 2"), info2, mockRow(2), 2))
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

  @Test
  void should_handle_failed_records_without_source() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(1),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    Record record = new DefaultErrorRecord(null, resource1, 1, new RuntimeException("error 1"));
    Flux<Record> stmts = Flux.just(record);
    stmts.transform(logManager.newFailedRecordsHandler()).blockLast();
    logManager.close();
    Path errors = logManager.getOperationDirectory().resolve("connector-errors.log");
    Path positions = logManager.getOperationDirectory().resolve("summary.csv");
    assertThat(errors.toFile()).exists();
    assertThat(positions.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(errors, positions);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .doesNotContain("Source: ")
        .contains("Resource: " + resource1)
        .contains("java.lang.RuntimeException: error 1");
    List<String> positionLines = Files.readAllLines(positions, UTF_8);
    assertThat(positionLines).contains("file:///file1.csv;[1,1];0");
  }

  @Test
  void should_handle_unmappable_statements_without_source() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(1),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    Record record = DefaultRecord.indexed(null, resource1, 1, "foo", " bar");
    UnmappableStatement stmt = new UnmappableStatement(record, new RuntimeException("error 1"));
    Flux<BatchableStatement<?>> stmts = Flux.just(stmt);
    stmts.transform(logManager.newUnmappableStatementsHandler()).blockLast();
    logManager.close();
    Path errors = logManager.getOperationDirectory().resolve("mapping-errors.log");
    Path positions = logManager.getOperationDirectory().resolve("summary.csv");
    assertThat(errors.toFile()).exists();
    assertThat(positions.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(errors, positions);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .doesNotContain("Source: ")
        .contains("Resource: " + resource1)
        .contains("Position: 1")
        .contains("java.lang.RuntimeException: error 1");
  }

  @Test
  void should_handle_unmappable_records_without_source() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(1),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();
    Record record =
        new DefaultErrorRecord(null, tableResource1, 1, new RuntimeException("error 1"));
    Flux<Record> stmts = Flux.just(record);
    stmts.transform(logManager.newUnmappableRecordsHandler()).blockLast();
    logManager.close();
    Path errors = logManager.getOperationDirectory().resolve("mapping-errors.log");
    Path positions = logManager.getOperationDirectory().resolve("summary.csv");
    assertThat(errors.toFile()).exists();
    assertThat(positions.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(errors, positions);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .doesNotContain("Source: ")
        .contains("Resource: cql://ks1/table1?start=1&end=2")
        .contains("Position: 1")
        .contains("java.lang.RuntimeException: error 1");
  }

  @Test
  void should_record_positions_and_successful_resources_when_loading() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(3),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();

    // Emulate connector parsing

    Flux<Record> records =
        Flux.just(
            // resource 1: 3 records total (resource done)
            DefaultRecord.indexed(source1, resource1, 1, "line1"),
            DefaultRecord.indexed(source2, resource1, 2, "line2"),
            DefaultRecord.indexed(source3, resource1, 3, "line3"),
            // resource 2: 3+ records, one not parsable (resource not done)
            DefaultRecord.indexed(source1, resource2, 1, "line1"),
            new DefaultErrorRecord(
                source2, resource2, 2, new RuntimeException("connector error line2")),
            DefaultRecord.indexed(source3, resource2, 3, "line3"));

    List<Record> goodRecords =
        records.transform(logManager.newFailedRecordsHandler()).collectList().block();

    assertThat(goodRecords).hasSize(5);
    assertThat(logManager.positionsTracker.getPositions()).containsOnlyKeys(resource2);
    assertThat(logManager.positionsTracker.getPositions().get(resource2))
        .containsExactly(new Range(2));

    // Emulate record->statement mapper

    Flux<BatchableStatement<?>> stmts =
        Flux.just(
            // resource 1: 3 statements total
            mockMappedBoundStatement(1, source1, resource1),
            mockMappedBoundStatement(2, source2, resource1),
            mockMappedBoundStatement(3, source3, resource1),
            // resource 2: 2 statements left, one unmappable
            new UnmappableStatement(
                DefaultRecord.indexed(source1, resource2, 1, "line1"),
                new RuntimeException("mapping error line1")),
            mockMappedBoundStatement(3, source1, resource3));

    List<BatchableStatement<?>> goodStmts =
        stmts.transform(logManager.newUnmappableStatementsHandler()).collectList().block();

    assertThat(goodStmts).hasSize(4);
    assertThat(logManager.positionsTracker.getPositions()).containsOnlyKeys(resource2);
    assertThat(logManager.positionsTracker.getPositions().get(resource2))
        .containsExactly(new Range(1, 2));

    // Emulate statement execution

    Flux<WriteResult> writes =
        Flux.just(
            // resource 1: 3 writes
            new DefaultWriteResult(
                mockMappedBoundStatement(1, source1, resource1),
                new MockAsyncResultSet(0, null, null)),
            new DefaultWriteResult(
                mockMappedBoundStatement(2, source2, resource1),
                new MockAsyncResultSet(0, null, null)),
            new DefaultWriteResult(
                mockMappedBoundStatement(3, source3, resource1),
                new MockAsyncResultSet(0, null, null)),
            // resource 2: 1 write left, which fails
            new DefaultWriteResult(
                new BulkExecutionException(
                    new DriverTimeoutException("load error 3"),
                    new MappedBoundStatement(
                        DefaultRecord.indexed(source3, resource2, 3, "line3"),
                        mockMappedBoundStatement(3, source3, resource2)))));

    List<WriteResult> goodWrites =
        writes.transform(logManager.newFailedWritesHandler()).collectList().block();

    assertThat(goodWrites).isNotNull().hasSize(3);
    assertThat(logManager.positionsTracker.getPositions()).containsOnlyKeys(resource2);
    assertThat(logManager.positionsTracker.getPositions().get(resource2))
        .containsExactly(new Range(1, 3));

    // Emulate signaling successful writes

    Flux.fromIterable(goodWrites)
        .transform(logManager.newWriteResultPositionsHandler())
        .blockLast();

    assertThat(logManager.positionsTracker.getPositions()).containsOnlyKeys(resource1, resource2);
    assertThat(logManager.positionsTracker.getPositions().get(resource1))
        .containsExactly(new Range(1, 3));
    assertThat(logManager.positionsTracker.getPositions().get(resource2))
        .containsExactly(new Range(1, 3));

    // Emulate connector signaling successful resources

    BiFunction<URI, Publisher<Record>, Publisher<Record>> connectorResourceHandler =
        logManager.newConnectorResourceHandler();
    Flux.from(connectorResourceHandler.apply(resource1, Flux.empty())).blockLast();
    // resource 2 not finished
    Flux.from(connectorResourceHandler.apply(resource3, Flux.empty())).blockLast();

    assertThat(logManager.finishedResources)
        .containsOnly(entry(resource1, true), entry(resource3, true));

    logManager.close();

    // Check connector files

    Path connectorBad = logManager.getOperationDirectory().resolve("connector.bad");
    assertThat(connectorBad.toFile()).exists();
    List<String> connectorBadLines = Files.readAllLines(connectorBad, UTF_8);
    assertThat(connectorBadLines).hasSize(1);
    assertThat(connectorBadLines.get(0)).isEqualTo(source2.trim());

    Path connectorErrors = logManager.getOperationDirectory().resolve("connector-errors.log");
    assertThat(connectorErrors.toFile()).exists();
    List<String> connectorErrorLines = Files.readAllLines(connectorErrors, UTF_8);
    assertThat(String.join("\n", connectorErrorLines))
        .containsOnlyOnce("Resource: " + resource2)
        .containsOnlyOnce("Position: 2")
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source2))
        .containsOnlyOnce("java.lang.RuntimeException: connector error line2");

    // Check mapping files

    Path mappingBad = logManager.getOperationDirectory().resolve("mapping.bad");
    assertThat(mappingBad.toFile()).exists();
    List<String> mappingBadLines = Files.readAllLines(mappingBad, UTF_8);
    assertThat(mappingBadLines).hasSize(1);
    assertThat(mappingBadLines.get(0)).isEqualTo(source1.trim());

    Path mappingErrors = logManager.getOperationDirectory().resolve("mapping-errors.log");
    assertThat(mappingErrors.toFile()).exists();
    List<String> mappingErrorLines = Files.readAllLines(mappingErrors, UTF_8);
    assertThat(String.join("\n", mappingErrorLines))
        .containsOnlyOnce("Resource: " + resource2)
        .containsOnlyOnce("Position: 1")
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source1))
        .containsOnlyOnce("java.lang.RuntimeException: mapping error line1");

    // Check load files

    Path loadBad = logManager.getOperationDirectory().resolve("load.bad");
    assertThat(loadBad.toFile()).exists();
    List<String> loadBadLines = Files.readAllLines(loadBad, UTF_8);
    assertThat(loadBadLines).hasSize(1);
    assertThat(loadBadLines.get(0)).isEqualTo(source3.trim());

    Path loadErrors = logManager.getOperationDirectory().resolve("load-errors.log");
    assertThat(loadErrors.toFile()).exists();
    List<String> loadErrorLines = Files.readAllLines(loadErrors, UTF_8);
    assertThat(String.join("\n", loadErrorLines))
        .containsOnlyOnce("Resource: " + resource2)
        .containsOnlyOnce("Position: 3")
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine(source3))
        .containsOnlyOnce(
            "BulkExecutionException: Statement execution failed: INSERT INTO 3 (load error 3)");

    // Check summary.csv

    Path summary = logManager.getOperationDirectory().resolve("summary.csv");
    assertThat(summary.toFile()).exists();
    List<String> summaryLines = Files.readAllLines(summary, UTF_8);
    assertThat(summaryLines)
        .containsExactly(
            "resource;ranges;done",
            "file:///file1.csv;[1,3];1",
            "file:///file2.csv;[1,3];0",
            "file:///file3.csv;;1");

    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(
            connectorBad, connectorErrors, mappingBad, mappingErrors, loadBad, loadErrors, summary);
  }

  @Test
  void should_record_positions_and_successful_resources_when_unloading() throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(3),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter);
    logManager.init();

    // Emulate statement execution

    TokenRange tokenRange1 = new Murmur3TokenRange(new Murmur3Token(1), new Murmur3Token(2));
    TokenRange tokenRange2 = new Murmur3TokenRange(new Murmur3Token(2), new Murmur3Token(3));
    TokenRange tokenRange3 = new Murmur3TokenRange(new Murmur3Token(3), new Murmur3Token(4));

    URI resource1 =
        RangeReadStatement.rangeReadResource(
            CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("t"), tokenRange1);
    URI resource2 =
        RangeReadStatement.rangeReadResource(
            CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("t"), tokenRange2);
    URI resource3 =
        RangeReadStatement.rangeReadResource(
            CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("t"), tokenRange3);

    RangeReadBoundStatement statement1 = mockRangeReadBoundStatement(tokenRange1);
    RangeReadBoundStatement statement2 = mockRangeReadBoundStatement(tokenRange2);
    RangeReadBoundStatement statement3 = mockRangeReadBoundStatement(tokenRange3);

    BulkExecutionException statement2Failure =
        new BulkExecutionException(new DriverTimeoutException("unload error 2"), statement2);

    // resource 1: 3 reads
    DefaultReadResult result1_1 =
        new DefaultReadResult(statement1, mock(ExecutionInfo.class), mockRow(1), 1);
    DefaultReadResult result1_2 =
        new DefaultReadResult(statement1, mock(ExecutionInfo.class), mockRow(2), 2);
    DefaultReadResult result1_3 =
        new DefaultReadResult(statement1, mock(ExecutionInfo.class), mockRow(3), 3);
    // resource 2: read fails
    DefaultReadResult result2 = new DefaultReadResult(statement2Failure);
    // resource 3: empty

    Flux<ReadResult> reads = Flux.just(result1_1, result1_2, result1_3, result2);

    List<ReadResult> goodReads =
        reads.transform(logManager.newFailedReadsHandler()).collectList().block();

    assertThat(goodReads).isNotNull().hasSize(3);
    assertThat(logManager.positionsTracker.getPositions()).isEmpty();

    // Emulate result->record mapper

    DefaultRecord record1_1 = new DefaultRecord(result1_1, resource1, 1);
    DefaultErrorRecord record1_2 =
        new DefaultErrorRecord(result1_2, resource1, 2, new RuntimeException("mapping error 2"));
    DefaultRecord record1_3 = new DefaultRecord(result1_3, resource1, 3);

    Flux<Record> records =
        Flux.just(
            // resource 1: 3 records total, one not parsable
            record1_1, record1_2, record1_3);

    List<Record> goodRecords =
        records.transform(logManager.newUnmappableRecordsHandler()).collectList().block();

    assertThat(goodRecords).isNotNull().hasSize(2);
    assertThat(logManager.positionsTracker.getPositions()).containsOnlyKeys(resource1);
    assertThat(logManager.positionsTracker.getPositions().get(resource1))
        .containsExactly(new Range(2));

    // Emulate record -> connector

    records =
        Flux.just(
            // resource 1: 2 records left, one nto writable
            new DefaultErrorRecord(
                result1_1, resource1, 1, new RuntimeException("connector error 1")),
            record1_3);

    goodRecords = records.transform(logManager.newFailedRecordsHandler()).collectList().block();

    assertThat(goodRecords).isNotNull().hasSize(1);
    assertThat(logManager.positionsTracker.getPositions()).containsOnlyKeys(resource1);
    assertThat(logManager.positionsTracker.getPositions().get(resource1))
        .containsExactly(new Range(1, 2));

    // Emulate signaling successful records

    Flux.fromIterable(goodRecords).transform(logManager.newRecordPositionsHandler()).blockLast();

    assertThat(logManager.positionsTracker.getPositions()).containsOnlyKeys(resource1);
    assertThat(logManager.positionsTracker.getPositions().get(resource1))
        .containsExactly(new Range(1, 3));

    // Emulate listener signaling successful and failed resources

    ExecutionListener listener = logManager.newReadExecutionListener();
    listener.onExecutionSuccessful(statement1, new DefaultExecutionContext());
    listener.onExecutionFailed(statement2Failure, new DefaultExecutionContext());
    listener.onExecutionSuccessful(statement3, new DefaultExecutionContext());

    assertThat(logManager.finishedResources)
        .containsOnly(entry(resource1, true), entry(resource2, false), entry(resource3, true));

    logManager.close();

    // Check unload files

    Path unloadErrors = logManager.getOperationDirectory().resolve("unload-errors.log");
    assertThat(unloadErrors.toFile()).exists();
    List<String> loadErrorLines = Files.readAllLines(unloadErrors, UTF_8);
    assertThat(String.join("\n", loadErrorLines))
        .containsOnlyOnce("Token Range: ]2,3]")
        .containsOnlyOnce(
            "BulkExecutionException: Statement execution failed: SELECT 2 (unload error 2)")
        .containsOnlyOnce(".DriverTimeoutException: unload error 2");

    // Check mapping files

    Path mappingBad = logManager.getOperationDirectory().resolve("mapping.bad");
    assertThat(mappingBad.toFile()).exists();
    List<String> mappingBadLines = Files.readAllLines(mappingBad, UTF_8);
    assertThat(mappingBadLines).hasSize(1);
    assertThat(mappingBadLines.get(0)).isEqualTo("[2]"); // mocked getFormattedContents

    Path mappingErrors = logManager.getOperationDirectory().resolve("mapping-errors.log");
    assertThat(mappingErrors.toFile()).exists();
    List<String> mappingErrorLines = Files.readAllLines(mappingErrors, UTF_8);
    assertThat(String.join("\n", mappingErrorLines))
        .containsOnlyOnce("Position: 2")
        .containsOnlyOnce("Token Range: ]1,2]")
        .containsOnlyOnce("SELECT 1")
        .containsOnlyOnce("Statement: RangeReadBoundStatement")
        .containsOnlyOnce("Row: Row")
        .containsOnlyOnce("java.lang.RuntimeException: mapping error 2");

    // Check connector files

    Path connectorBad = logManager.getOperationDirectory().resolve("connector.bad");
    assertThat(connectorBad.toFile()).exists();
    List<String> connectorBadLines = Files.readAllLines(connectorBad, UTF_8);
    assertThat(connectorBadLines).hasSize(1);
    assertThat(connectorBadLines.get(0)).isEqualTo("[1]"); // mocked getFormattedContents

    Path connectorErrors = logManager.getOperationDirectory().resolve("connector-errors.log");
    assertThat(connectorErrors.toFile()).exists();
    List<String> connectorErrorLines = Files.readAllLines(connectorErrors, UTF_8);
    assertThat(String.join("\n", connectorErrorLines))
        .containsOnlyOnce("Resource: " + resource1)
        .containsOnlyOnce("Position: 1")
        .containsOnlyOnce("Token Range: ]1,2]")
        .containsOnlyOnce("SELECT 1")
        .containsOnlyOnce("Statement: RangeReadBoundStatement")
        .containsOnlyOnce("Row: Row")
        .containsOnlyOnce("java.lang.RuntimeException: connector error 1");

    // Check summary.csv

    Path summary = logManager.getOperationDirectory().resolve("summary.csv");
    assertThat(summary.toFile()).exists();
    List<String> summaryLines = Files.readAllLines(summary, UTF_8);
    assertThat(summaryLines)
        .containsExactly(
            "resource;ranges;done",
            "cql://ks/t?start=1&end=2;[1,3];1", // 3 records processed, finished
            "cql://ks/t?start=2&end=3;;0", // no records processed, failed
            "cql://ks/t?start=3&end=4;;1" // no records processed, finished (empty)
            );

    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(
            connectorBad, connectorErrors, mappingBad, mappingErrors, unloadErrors, summary);
  }

  private static MappedBoundStatement mockMappedBoundStatement(
      int value, Object source, URI resource) {
    BoundStatement bs = mockBoundStatement("INSERT INTO " + value, value);
    return new MappedBoundStatement(DefaultRecord.indexed(source, resource, value, value), bs);
  }

  private static RangeReadBoundStatement mockRangeReadBoundStatement(TokenRange range) {
    BoundStatement bs = mockBoundStatement("SELECT " + TokenUtils.getTokenValue(range.getStart()));
    URI resource =
        RangeReadStatement.rangeReadResource(
            CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("t"), range);
    return new RangeReadBoundStatement(bs, range, resource);
  }
}
