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
import static com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.ReplayStrategy.resume;
import static java.nio.charset.StandardCharsets.UTF_8;
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
import com.datastax.oss.dsbulk.connectors.api.DefaultResource;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.connectors.api.Resource;
import com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException;
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
import com.datastax.oss.dsbulk.tests.utils.ReflectionUtils;
import com.datastax.oss.dsbulk.workflow.api.error.AbsoluteErrorThreshold;
import com.datastax.oss.dsbulk.workflow.api.error.ErrorThreshold;
import com.datastax.oss.dsbulk.workflow.api.error.RatioErrorThreshold;
import com.datastax.oss.dsbulk.workflow.api.error.TooManyErrorsException;
import com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.Checkpoint;
import com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.CheckpointManager;
import com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.Range;
import com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.ReplayStrategy;
import com.datastax.oss.dsbulk.workflow.commons.statement.MappedBoundStatement;
import com.datastax.oss.dsbulk.workflow.commons.statement.MappedBoundStatementPrinter;
import com.datastax.oss.dsbulk.workflow.commons.statement.RangeReadBoundStatement;
import com.datastax.oss.dsbulk.workflow.commons.statement.RangeReadStatement;
import com.datastax.oss.dsbulk.workflow.commons.statement.UnmappableStatement;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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
          .addStatementPrinters(new MappedBoundStatementPrinter())
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
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
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors);
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
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
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors);
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
    logManager.init();
    Flux<BatchableStatement<?>> stmts =
        Flux.just(unmappableStmt1, unmappableStmt2, unmappableStmt3);
    // should not throw TooManyErrorsException
    stmts.transform(logManager.newUnmappableStatementsHandler()).blockLast();
    logManager.close();
    Path bad = logManager.getOperationDirectory().resolve("mapping.bad");
    Path errors = logManager.getOperationDirectory().resolve("mapping-errors.log");
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors);
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
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
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors);
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
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
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    List<String> badLines = Files.readAllLines(bad, UTF_8);
    assertThat(badLines).hasSize(3);
    assertThat(badLines.get(0)).isEqualTo(source1.trim());
    assertThat(badLines.get(1)).isEqualTo(source2.trim());
    assertThat(badLines.get(2)).isEqualTo(source3.trim());
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors);
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
    logManager.init();
    Flux<WriteResult> stmts = Flux.just(failedWriteResult1, failedWriteResult2, failedWriteResult3);
    stmts.transform(logManager.newFailedWritesHandler()).blockLast();
    logManager.close();
    Path bad = logManager.getOperationDirectory().resolve("load.bad");
    Path errors = logManager.getOperationDirectory().resolve("load-errors.log");
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    List<String> badLines = Files.readAllLines(bad, UTF_8);
    assertThat(badLines).hasSize(3);
    assertThat(badLines.get(0)).isEqualTo(source1.trim());
    assertThat(badLines.get(1)).isEqualTo(source2.trim());
    assertThat(badLines.get(2)).isEqualTo(source3.trim());
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors);
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
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
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    List<String> badLines = Files.readAllLines(bad, UTF_8);
    assertThat(badLines).hasSize(3);
    assertThat(badLines.get(0)).isEqualTo(source1.trim());
    assertThat(badLines.get(1)).isEqualTo(source2.trim());
    assertThat(badLines.get(2)).isEqualTo(source3.trim());
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors);
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
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
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(errors, bad);
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
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
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(errors, bad);
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
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
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    List<String> badLines = Files.readAllLines(bad, UTF_8);
    assertThat(badLines).hasSize(1);
    assertThat(badLines.get(0)).isEqualTo(source1.trim());
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors);
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
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
    assertThat(bad.toFile()).exists();
    assertThat(errors.toFile()).exists();
    List<String> badLines = Files.readAllLines(bad, UTF_8);
    assertThat(badLines).hasSize(3);
    assertThat(badLines.get(0)).isEqualTo(source1.trim());
    assertThat(badLines.get(1)).isEqualTo(source2.trim());
    assertThat(badLines.get(2)).isEqualTo(source3.trim());
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(bad, errors);
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
    logManager.init();
    Record record = new DefaultErrorRecord(null, resource1, 1, new RuntimeException("error 1"));
    Flux<Record> stmts = Flux.just(record);
    stmts.transform(logManager.newFailedRecordsHandler()).blockLast();
    logManager.close();
    Path errors = logManager.getOperationDirectory().resolve("connector-errors.log");
    assertThat(errors.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(errors);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .doesNotContain("Source: ")
        .contains("Resource: " + resource1)
        .contains("java.lang.RuntimeException: error 1");
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
    logManager.init();
    Record record = DefaultRecord.indexed(null, resource1, 1, "foo", " bar");
    UnmappableStatement stmt = new UnmappableStatement(record, new RuntimeException("error 1"));
    Flux<BatchableStatement<?>> stmts = Flux.just(stmt);
    stmts.transform(logManager.newUnmappableStatementsHandler()).blockLast();
    logManager.close();
    Path errors = logManager.getOperationDirectory().resolve("mapping-errors.log");
    assertThat(errors.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(errors);
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
            rowFormatter,
            true,
            new CheckpointManager(),
            resume);
    logManager.init();
    Record record =
        new DefaultErrorRecord(null, tableResource1, 1, new RuntimeException("error 1"));
    Flux<Record> stmts = Flux.just(record);
    stmts.transform(logManager.newUnmappableRecordsHandler()).blockLast();
    logManager.close();
    Path errors = logManager.getOperationDirectory().resolve("mapping-errors.log");
    assertThat(errors.toFile()).exists();
    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(errors);
    List<String> lines = Files.readAllLines(errors, UTF_8);
    String content = String.join("\n", lines);
    assertThat(content)
        .doesNotContain("Source: ")
        .contains("Resource: cql://ks1/table1?start=1&end=2")
        .contains("Position: 1")
        .contains("java.lang.RuntimeException: error 1");
  }

  @ParameterizedTest
  @EnumSource(ReplayStrategy.class)
  void should_resume_operation_when_loading(ReplayStrategy strategy) throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(3),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter,
            true,
            new CheckpointManager(),
            strategy);
    logManager.init();

    Record record1_1 = DefaultRecord.indexed("line1", resource1, 1, "line1");
    Record record1_2 = DefaultRecord.indexed("line2", resource1, 2, "line2");
    Record record1_3 = DefaultRecord.indexed("line3", resource1, 3, "line3");

    Record record2_1 = DefaultRecord.indexed("line1", resource2, 1, "line1");
    Record record2_2 =
        new DefaultErrorRecord(
            "line2", resource2, 2, new RuntimeException("connector error line2"));
    Record record2_3 = DefaultRecord.indexed("line3", resource2, 3, "line3");
    Record record2_4 = DefaultRecord.indexed("line3", resource2, 4, "line4");

    Resource res1 =
        new DefaultResource(
            resource1, Flux.defer(() -> Flux.just(record1_1, record1_2, record1_3)));
    Resource res2 =
        new DefaultResource(
            resource2, Flux.defer(() -> Flux.just(record2_1, record2_2, record2_3, record2_4)));
    Resource res3 = new DefaultResource(resource3, Flux.empty());

    MappedBoundStatement stmt1_1 = mockMappedBoundStatement(1, "line1", resource1);
    MappedBoundStatement stmt1_2 = mockMappedBoundStatement(2, "line2", resource1);
    MappedBoundStatement stmt1_3 = mockMappedBoundStatement(3, "line3", resource1);

    UnmappableStatement stmt2_1 =
        new UnmappableStatement(record2_1, new RuntimeException("mapping error line1"));
    MappedBoundStatement stmt2_3 = mockMappedBoundStatement(3, "line3", resource2);

    MockAsyncResultSet rs = new MockAsyncResultSet(0, null, null);

    // Emulate connector reading
    Flux.just(res1, res2, res3)
        .transform(logManager.newConnectorCheckpointHandler())
        .concatMap(r -> r)
        // Emulate an unfinished resource, i.e., not all its records were processed when the
        // operation stopped.
        .filter(r -> !r.equals(record2_4))
        .transform(logManager.newFailedRecordsHandler())

        // Emulate record->statement mapper
        .<BatchableStatement<?>>map(
            record -> {
              // resource 1: 3 statements
              if (record.equals(record1_1)) {
                return stmt1_1;
              } else if (record.equals(record1_2)) {
                return stmt1_2;
              } else if (record.equals(record1_3)) {
                return stmt1_3;
              }
              // resource 2: 2 statements, one unmappable
              if (record.equals(record2_1)) {
                return stmt2_1;
              } else if (record.equals(record2_3)) {
                return stmt2_3;
              }
              throw new AssertionError();
              // resource 3
            })
        .transform(logManager.newUnmappableStatementsHandler())

        // Emulate statement execution
        .<WriteResult>map(
            stmt -> {
              // resource 1: 3 writes
              if (stmt == stmt1_1) {
                return new DefaultWriteResult(stmt1_1, rs);
              } else if (stmt == stmt1_2) {
                return new DefaultWriteResult(stmt1_2, rs);
              } else if (stmt == stmt1_3) {
                return new DefaultWriteResult(stmt1_3, rs);
              }
              // resource 2: 1 write left, which fails
              if (stmt == stmt2_3) {
                return new DefaultWriteResult(
                    new BulkExecutionException(
                        new DriverTimeoutException("load error 3"), stmt2_3));
              }
              throw new AssertionError();
            })
        .transform(logManager.newFailedWritesHandler())
        .transform(logManager.newSuccessfulWritesHandler())
        .blockLast();

    logManager.close();

    CheckpointManager checkpointManager = logManager.mergeCheckpointManagers();
    Map<URI, Checkpoint> checkpoints = getCheckpoints(checkpointManager);

    assertThat(checkpoints).containsOnlyKeys(resource1, resource2, resource3);

    assertThat(resume.isComplete(checkpoints.get(resource1))).isTrue();
    assertThat(resume.isComplete(checkpoints.get(resource2))).isFalse();
    assertThat(resume.isComplete(checkpoints.get(resource3))).isTrue();

    assertThat(checkpoints.get(resource1).getProduced()).isEqualTo(3L);
    assertThat(checkpoints.get(resource2).getProduced()).isEqualTo(4L);
    assertThat(checkpoints.get(resource3).getProduced()).isEqualTo(0L);

    assertThat(checkpoints.get(resource1).getConsumedSuccessful().stream())
        .containsExactly(new Range(1, 3));
    assertThat(checkpoints.get(resource2).getConsumedSuccessful().stream()).isEmpty();
    assertThat(checkpoints.get(resource3).getConsumedSuccessful().stream()).isEmpty();

    assertThat(checkpoints.get(resource1).getConsumedFailed().stream()).isEmpty();
    assertThat(checkpoints.get(resource2).getConsumedFailed().stream())
        .containsExactly(new Range(1, 3));
    assertThat(checkpoints.get(resource3).getConsumedFailed().stream()).isEmpty();

    // Check connector files

    Path connectorBad = logManager.getOperationDirectory().resolve("connector.bad");
    assertThat(connectorBad.toFile()).exists();
    List<String> connectorBadLines = Files.readAllLines(connectorBad, UTF_8);
    assertThat(connectorBadLines).hasSize(1);
    assertThat(connectorBadLines.get(0)).isEqualTo("line2".trim());

    Path connectorErrors = logManager.getOperationDirectory().resolve("connector-errors.log");
    assertThat(connectorErrors.toFile()).exists();
    List<String> connectorErrorLines = Files.readAllLines(connectorErrors, UTF_8);
    assertThat(String.join("\n", connectorErrorLines))
        .containsOnlyOnce("Resource: " + resource2)
        .containsOnlyOnce("Position: 2")
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine("line2"))
        .containsOnlyOnce("java.lang.RuntimeException: connector error line2");

    // Check mapping files

    Path mappingBad = logManager.getOperationDirectory().resolve("mapping.bad");
    assertThat(mappingBad.toFile()).exists();
    List<String> mappingBadLines = Files.readAllLines(mappingBad, UTF_8);
    assertThat(mappingBadLines).hasSize(1);
    assertThat(mappingBadLines.get(0)).isEqualTo("line1".trim());

    Path mappingErrors = logManager.getOperationDirectory().resolve("mapping-errors.log");
    assertThat(mappingErrors.toFile()).exists();
    List<String> mappingErrorLines = Files.readAllLines(mappingErrors, UTF_8);
    assertThat(String.join("\n", mappingErrorLines))
        .containsOnlyOnce("Resource: " + resource2)
        .containsOnlyOnce("Position: 1")
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine("line1"))
        .containsOnlyOnce("java.lang.RuntimeException: mapping error line1");

    // Check load files

    Path loadBad = logManager.getOperationDirectory().resolve("load.bad");
    assertThat(loadBad.toFile()).exists();
    List<String> loadBadLines = Files.readAllLines(loadBad, UTF_8);
    assertThat(loadBadLines).hasSize(1);
    assertThat(loadBadLines.get(0)).isEqualTo("line3".trim());

    Path loadErrors = logManager.getOperationDirectory().resolve("load-errors.log");
    assertThat(loadErrors.toFile()).exists();
    List<String> loadErrorLines = Files.readAllLines(loadErrors, UTF_8);
    assertThat(String.join("\n", loadErrorLines))
        .containsOnlyOnce("Resource: " + resource2)
        .containsOnlyOnce("Position: 3")
        .containsOnlyOnce("Source: " + LogManagerUtils.formatSingleLine("line3"))
        .containsOnlyOnce(
            "BulkExecutionException: Statement execution failed: INSERT INTO 3 (load error 3)");

    // Check checkpoint.csv

    logManager.writeCheckpointFile(checkpointManager);
    Path checkpointFile = logManager.getOperationDirectory().resolve("checkpoint.csv");
    assertThat(checkpointFile.toFile()).exists();
    List<String> checkpointLines = Files.readAllLines(checkpointFile, UTF_8);
    assertThat(checkpointLines)
        .containsExactly(
            "file:///file1.csv;1;3;1:3;", "file:///file2.csv;1;4;;1:3", "file:///file3.csv;1;0;;");

    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(
            connectorBad,
            connectorErrors,
            mappingBad,
            mappingErrors,
            loadBad,
            loadErrors,
            checkpointFile);

    // Resume the failed operation

    outputDir = Files.createTempDirectory("test");
    logManager =
        new LogManager(
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(3),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter,
            true,
            checkpointManager,
            strategy);

    logManager.init();

    Record record2_2a = DefaultRecord.indexed("line2", resource2, 2, "line2");

    MappedBoundStatement stmt2_1a = mockMappedBoundStatement(1, "line2", resource2);
    MappedBoundStatement stmt2_2 = mockMappedBoundStatement(2, "line2", resource2);
    MappedBoundStatement stmt2_3a = mockMappedBoundStatement(3, "line3", resource2);
    MappedBoundStatement stmt2_4 = mockMappedBoundStatement(4, "line4", resource2);

    res2 = new DefaultResource(resource2, Flux.just(record2_1, record2_2a, record2_3, record2_4));

    // Only the unfinished resource2 should be read again.
    // Emulate connector reading
    Flux.just(res1, res2, res3)
        .transform(logManager.newConnectorCheckpointHandler())
        .concatMap(r -> r)
        .transform(logManager.newFailedRecordsHandler())

        // Emulate record->statement mapper
        .<BatchableStatement<?>>map(
            record -> {
              switch (strategy) {
                case retry:
                case rewind:
                  if (record.equals(record2_1)) {
                    return stmt2_1a;
                  }
                  if (record.equals(record2_2a)) {
                    return stmt2_2;
                  }
                  if (record.equals(record2_3)) {
                    return stmt2_3a;
                  }
                  // fall through
                case resume:
                  if (record.equals(record2_4)) {
                    return stmt2_4;
                  }
                  break;
              }
              throw new AssertionError();
              // resource 3
            })
        .transform(logManager.newUnmappableStatementsHandler())

        // Emulate statement execution
        .<WriteResult>map(
            stmt -> {
              switch (strategy) {
                case retry:
                case rewind:
                  if (stmt == stmt2_1a) {
                    return new DefaultWriteResult(stmt2_1a, rs);
                  }
                  if (stmt == stmt2_2) {
                    return new DefaultWriteResult(stmt2_2, rs);
                  }
                  if (stmt == stmt2_3a) {
                    return new DefaultWriteResult(stmt2_3a, rs);
                  }
                  // fall through
                case resume:
                  if (stmt == stmt2_4) {
                    return new DefaultWriteResult(stmt2_4, rs);
                  }
                  break;
              }

              throw new AssertionError();
            })
        .transform(logManager.newFailedWritesHandler())
        .transform(logManager.newSuccessfulWritesHandler())
        .blockLast();

    logManager.close();

    checkpointManager = logManager.mergeCheckpointManagers();

    checkpoints = getCheckpoints(checkpointManager);

    assertThat(checkpoints).containsOnlyKeys(resource1, resource2, resource3);

    assertThat(resume.isComplete(checkpoints.get(resource1))).isTrue();
    assertThat(resume.isComplete(checkpoints.get(resource2))).isTrue();
    assertThat(resume.isComplete(checkpoints.get(resource3))).isTrue();

    assertThat(checkpoints.get(resource1).getProduced()).isEqualTo(3L);
    assertThat(checkpoints.get(resource2).getProduced()).isEqualTo(4L);
    assertThat(checkpoints.get(resource3).getProduced()).isEqualTo(0L);

    assertThat(checkpoints.get(resource1).getConsumedSuccessful().stream())
        .containsExactly(new Range(1, 3));
    switch (strategy) {
      case retry:
      case rewind:
        assertThat(checkpoints.get(resource2).getConsumedSuccessful().stream())
            .containsExactly(new Range(1, 4));
        assertThat(checkpoints.get(resource3).getConsumedSuccessful().stream()).isEmpty();

        assertThat(checkpoints.get(resource1).getConsumedFailed().stream()).isEmpty();
        assertThat(checkpoints.get(resource2).getConsumedFailed().stream()).isEmpty();
        assertThat(checkpoints.get(resource3).getConsumedFailed().stream()).isEmpty();
        break;
      case resume:
        assertThat(checkpoints.get(resource2).getConsumedSuccessful().stream())
            .containsExactly(new Range(4, 4));
        assertThat(checkpoints.get(resource3).getConsumedSuccessful().stream()).isEmpty();

        assertThat(checkpoints.get(resource1).getConsumedFailed().stream()).isEmpty();
        assertThat(checkpoints.get(resource2).getConsumedFailed().stream())
            .containsExactly(new Range(1, 3));
        assertThat(checkpoints.get(resource3).getConsumedFailed().stream()).isEmpty();
        break;
    }

    // Check checkpoint.csv

    logManager.writeCheckpointFile(checkpointManager);
    checkpointFile = logManager.getOperationDirectory().resolve("checkpoint.csv");
    assertThat(checkpointFile.toFile()).exists();
    checkpointLines = Files.readAllLines(checkpointFile, UTF_8);
    switch (strategy) {
      case retry:
      case rewind:
        assertThat(checkpointLines)
            .containsExactly(
                "file:///file1.csv;1;3;1:3;",
                "file:///file2.csv;1;4;1:4;",
                "file:///file3.csv;1;0;;");
        break;
      case resume:
        assertThat(checkpointLines)
            .containsExactly(
                "file:///file1.csv;1;3;1:3;",
                "file:///file2.csv;1;4;4;1:3",
                "file:///file3.csv;1;0;;");
        break;
    }

    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(checkpointFile);
  }

  @ParameterizedTest
  @EnumSource(ReplayStrategy.class)
  void should_resume_operation_when_unloading(ReplayStrategy strategy) throws Exception {
    Path outputDir = Files.createTempDirectory("test");
    LogManager logManager =
        new LogManager(
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(3),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter,
            true,
            new CheckpointManager(),
            strategy);
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

    ReadResult result1_1 =
        new DefaultReadResult(statement1, mock(ExecutionInfo.class), mockRow(1), 1);
    ReadResult result1_2 =
        new DefaultReadResult(statement1, mock(ExecutionInfo.class), mockRow(2), 2);
    ReadResult result1_3 =
        new DefaultReadResult(statement1, mock(ExecutionInfo.class), mockRow(3), 3);

    BulkExecutionException statement2Failure =
        new BulkExecutionException(new DriverTimeoutException("unload error 2"), statement2);
    ReadResult result2 = new DefaultReadResult(statement2Failure);

    Record record1_1 = new DefaultRecord(result1_1, resource1, 1);
    Record record1_2 =
        new DefaultErrorRecord(result1_2, resource1, 2, new RuntimeException("mapping error 2"));
    Record record1_3 = new DefaultRecord(result1_3, resource1, 3);

    // resource 1: 3 reads
    RangeReadResource res1 =
        mockRangeReadResource(
            resource1, Flux.defer(() -> Flux.just(result1_1, result1_2, result1_3)));
    // resource 2: read fails
    RangeReadResource res2 = mockRangeReadResource(resource2, Flux.just(result2));
    // resource 3: empty
    RangeReadResource res3 = mockRangeReadResource(resource3, Flux.empty());

    Flux.just(res1, res2, res3)
        .transform(logManager.newRangeReadCheckpointHandler())
        .flatMap(r -> r)
        .transform(logManager.newFailedReadsHandler())

        // Emulate read result -> record mapper
        .map(
            result -> {
              if (result.equals(result1_1)) {
                return record1_1;
              } else if (result.equals(result1_2)) {
                return record1_2;
              } else if (result.equals(result1_3)) {
                return record1_3;
              }
              throw new AssertionError();
            })
        .transform(logManager.newUnmappableRecordsHandler())

        // Emulate connector write
        .map(
            record -> {
              if (record.equals(record1_1)) {
                return new DefaultErrorRecord(
                    record.getSource(),
                    record.getResource(),
                    record.getPosition(),
                    new RuntimeException("connector error 1"));
              } else if (record.equals(record1_3)) {
                return record;
              }
              throw new AssertionError();
            })
        .transform(logManager.newFailedRecordsHandler())
        .transform(logManager.newSuccessfulRecordsHandler())
        .blockLast();

    logManager.close();

    CheckpointManager checkpointManager = logManager.mergeCheckpointManagers();
    Map<URI, Checkpoint> checkpoints = getCheckpoints(checkpointManager);

    assertThat(checkpoints).containsOnlyKeys(resource1, resource2, resource3);

    assertThat(resume.isComplete(checkpoints.get(resource1))).isTrue();
    assertThat(resume.isComplete(checkpoints.get(resource2))).isFalse();
    assertThat(resume.isComplete(checkpoints.get(resource3))).isTrue();

    assertThat(checkpoints.get(resource1).getProduced()).isEqualTo(3L);
    assertThat(checkpoints.get(resource2).getProduced()).isEqualTo(0L);
    assertThat(checkpoints.get(resource3).getProduced()).isEqualTo(0L);

    assertThat(checkpoints.get(resource1).getConsumedSuccessful().stream())
        .containsExactly(new Range(3, 3));
    assertThat(checkpoints.get(resource2).getConsumedSuccessful().stream()).isEmpty();
    assertThat(checkpoints.get(resource3).getConsumedSuccessful().stream()).isEmpty();

    assertThat(checkpoints.get(resource1).getConsumedFailed().stream())
        .containsExactly(new Range(1, 2));
    assertThat(checkpoints.get(resource2).getConsumedFailed().stream()).isEmpty();
    assertThat(checkpoints.get(resource3).getConsumedFailed().stream()).isEmpty();

    // Check unload files

    Path unloadErrors = logManager.getOperationDirectory().resolve("unload-errors.log");
    assertThat(unloadErrors.toFile()).exists();
    List<String> loadErrorLines = Files.readAllLines(unloadErrors, UTF_8);
    assertThat(String.join("\n", loadErrorLines))
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
        .containsOnlyOnce("SELECT 1")
        .containsOnlyOnce("Statement: RangeReadBoundStatement")
        .containsOnlyOnce("Row: Row")
        .containsOnlyOnce("java.lang.RuntimeException: connector error 1");

    // Check checkpoint.csv

    logManager.writeCheckpointFile(checkpointManager);
    Path checkpointFile = logManager.getOperationDirectory().resolve("checkpoint.csv");
    assertThat(checkpointFile.toFile()).exists();
    List<String> checkpointLines = Files.readAllLines(checkpointFile, UTF_8);
    assertThat(checkpointLines)
        .containsExactly(
            "cql://ks/t?start=1&end=2;1;3;3;1:2", // 3 records processed, 2 failed, finished
            "cql://ks/t?start=2&end=3;0;0;;", // no records processed, failed
            "cql://ks/t?start=3&end=4;1;0;;" // no records processed, finished (empty)
            );

    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(
            connectorBad, connectorErrors, mappingBad, mappingErrors, unloadErrors, checkpointFile);

    // Resume the failed operation

    outputDir = Files.createTempDirectory("test");
    logManager =
        new LogManager(
            session,
            outputDir,
            ErrorThreshold.forAbsoluteValue(3),
            ErrorThreshold.forAbsoluteValue(0),
            statementFormatter,
            EXTENDED,
            rowFormatter,
            true,
            checkpointManager,
            strategy);

    logManager.init();

    ReadResult result2_1 =
        new DefaultReadResult(statement2, mock(ExecutionInfo.class), mockRow(1), 1);
    ReadResult result2_2 =
        new DefaultReadResult(statement2, mock(ExecutionInfo.class), mockRow(2), 2);
    ReadResult result2_3 =
        new DefaultReadResult(statement2, mock(ExecutionInfo.class), mockRow(3), 3);

    Record record2_1 = new DefaultRecord(result2_1, resource2, 1);
    Record record2_2 = new DefaultRecord(result2_2, resource2, 2);
    Record record2_3 = new DefaultRecord(result2_3, resource2, 3);

    Record record1_2a = new DefaultRecord(result1_2, resource1, 2);

    res2 = mockRangeReadResource(resource2, Flux.just(result2_1, result2_2, result2_3));

    Flux.just(res1, res2, res3)
        .transform(logManager.newRangeReadCheckpointHandler())
        .flatMap(r -> r)
        .transform(logManager.newFailedReadsHandler())

        // Emulate read result -> record mapper
        .map(
            result -> {
              switch (strategy) {
                case rewind:
                  if (result.equals(result1_3)) {
                    return record1_3;
                  }
                  // fall through
                case retry:
                  if (result.equals(result1_1)) {
                    return record1_1;
                  }
                  if (result.equals(result1_2)) {
                    return record1_2a;
                  }
                  // fall through
                case resume:
                  if (result.equals(result2_1)) {
                    return record2_1;
                  }
                  if (result.equals(result2_2)) {
                    return record2_2;
                  }
                  if (result.equals(result2_3)) {
                    return record2_3;
                  }
                  break;
              }
              throw new AssertionError();
            })
        .transform(logManager.newUnmappableRecordsHandler())

        // Emulate connector write
        .map(
            record -> {
              switch (strategy) {
                case rewind:
                  if (record.equals(record1_3)) {
                    return record1_3;
                  }
                  // fall through
                case retry:
                  if (record.equals(record1_1)) {
                    return record1_1;
                  }
                  if (record.equals(record1_2a)) {
                    return record1_2a;
                  }
                  // fall through
                case resume:
                  if (record.equals(record2_1)) {
                    return record2_1;
                  }
                  if (record.equals(record2_2)) {
                    return record2_2;
                  }
                  if (record.equals(record2_3)) {
                    return record2_3;
                  }
                  break;
              }
              throw new AssertionError();
            })
        .transform(logManager.newFailedRecordsHandler())
        .transform(logManager.newSuccessfulRecordsHandler())
        .blockLast();

    logManager.close();

    checkpointManager = logManager.mergeCheckpointManagers();
    checkpoints = getCheckpoints(checkpointManager);

    assertThat(checkpoints).containsOnlyKeys(resource1, resource2, resource3);

    assertThat(resume.isComplete(checkpoints.get(resource1))).isTrue();
    assertThat(resume.isComplete(checkpoints.get(resource2))).isTrue();
    assertThat(resume.isComplete(checkpoints.get(resource3))).isTrue();

    assertThat(checkpoints.get(resource1).getProduced()).isEqualTo(3L);
    assertThat(checkpoints.get(resource2).getProduced()).isEqualTo(3L);
    assertThat(checkpoints.get(resource3).getProduced()).isEqualTo(0L);

    switch (strategy) {
      case retry:
      case rewind:
        assertThat(checkpoints.get(resource1).getConsumedSuccessful().stream())
            .containsExactly(new Range(1, 3));
        assertThat(checkpoints.get(resource2).getConsumedSuccessful().stream())
            .containsExactly(new Range(1, 3));
        assertThat(checkpoints.get(resource3).getConsumedSuccessful().stream()).isEmpty();

        assertThat(checkpoints.get(resource1).getConsumedFailed().stream()).isEmpty();
        assertThat(checkpoints.get(resource2).getConsumedFailed().stream()).isEmpty();
        assertThat(checkpoints.get(resource3).getConsumedFailed().stream()).isEmpty();
        break;
      case resume:
        assertThat(checkpoints.get(resource1).getConsumedSuccessful().stream())
            .containsExactly(new Range(3, 3));
        assertThat(checkpoints.get(resource2).getConsumedSuccessful().stream())
            .containsExactly(new Range(1, 3));
        assertThat(checkpoints.get(resource3).getConsumedSuccessful().stream()).isEmpty();

        assertThat(checkpoints.get(resource1).getConsumedFailed().stream())
            .containsExactly(new Range(1, 2));
        assertThat(checkpoints.get(resource2).getConsumedFailed().stream()).isEmpty();
        assertThat(checkpoints.get(resource3).getConsumedFailed().stream()).isEmpty();
        break;
    }

    // Check checkpoint.csv

    logManager.writeCheckpointFile(checkpointManager);
    checkpointFile = logManager.getOperationDirectory().resolve("checkpoint.csv");
    assertThat(checkpointFile.toFile()).exists();
    checkpointLines = Files.readAllLines(checkpointFile, UTF_8);

    switch (strategy) {
      case retry:
      case rewind:
        assertThat(checkpointLines)
            .containsExactly(
                "cql://ks/t?start=1&end=2;1;3;1:3;",
                "cql://ks/t?start=2&end=3;1;3;1:3;",
                "cql://ks/t?start=3&end=4;1;0;;");
        break;
      case resume:
        assertThat(checkpointLines)
            .containsExactly(
                "cql://ks/t?start=1&end=2;1;3;3;1:2",
                "cql://ks/t?start=2&end=3;1;3;1:3;",
                "cql://ks/t?start=3&end=4;1;0;;");
        break;
    }

    assertThat(FileUtils.listAllFilesInDirectory(logManager.getOperationDirectory()))
        .containsOnly(checkpointFile);
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

  private static RangeReadResource mockRangeReadResource(URI resource, Flux<ReadResult> results) {
    RangeReadResource mock = mock(RangeReadResource.class);
    when(mock.getURI()).thenReturn(resource);
    when(mock.read()).thenReturn(results);
    return mock;
  }

  @SuppressWarnings("unchecked")
  private static Map<URI, Checkpoint> getCheckpoints(CheckpointManager checkpointManager) {
    return (Map<URI, Checkpoint>)
        ReflectionUtils.getInternalState(checkpointManager, "checkpoints");
  }
}
