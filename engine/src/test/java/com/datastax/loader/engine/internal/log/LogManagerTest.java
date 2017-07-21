/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.log;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.loader.connectors.api.Record;
import com.datastax.loader.connectors.api.internal.ErrorRecord;
import com.datastax.loader.engine.internal.log.statement.StatementFormatter;
import com.datastax.loader.engine.internal.schema.UnmappableStatement;
import com.datastax.loader.executor.api.exception.BulkExecutionException;
import com.datastax.loader.executor.api.internal.result.DefaultReadResult;
import com.datastax.loader.executor.api.result.Result;
import com.datastax.loader.executor.api.statement.BulkSimpleStatement;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.reactivex.Flowable;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

import static com.datastax.loader.engine.internal.log.statement.StatementFormatVerbosity.EXTENDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** */
@SuppressWarnings("FieldCanBeLocal")
public class LogManagerTest {

  private String source1 = "line1\n";
  private String source2 = "line2\n";
  private String source3 = "line3\n";

  private URL location1;
  private URL location2;
  private URL location3;

  private Record record1;
  private Record record2;
  private Record record3;

  private Statement stmt1;
  private Statement stmt2;
  private Statement stmt3;

  private Result result1;
  private Result result2;
  private Result result3;
  private Result batchResult;

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
    URL file1 = Files.createTempFile("file1_", ".csv").toFile().toURI().toURL();
    URL file2 = Files.createTempFile("file2_", ".csv").toFile().toURI().toURL();
    URL file3 = Files.createTempFile("file3_", ".csv").toFile().toURI().toURL();
    location1 = new URL(file1.toExternalForm() + "?line=1");
    location2 = new URL(file2.toExternalForm() + "?line=2");
    location3 = new URL(file3.toExternalForm() + "?line=3");
    record1 = new ErrorRecord(source1, this.location1, new RuntimeException("error 1"));
    record2 = new ErrorRecord(source2, this.location2, new RuntimeException("error 2"));
    record3 = new ErrorRecord(source3, this.location3, new RuntimeException("error 3"));
    stmt1 = new UnmappableStatement(location1, record1, new RuntimeException("error 1"));
    stmt2 = new UnmappableStatement(location2, record2, new RuntimeException("error 2"));
    stmt3 = new UnmappableStatement(location3, record3, new RuntimeException("error 3"));
    result1 =
        new DefaultReadResult(
            new BulkExecutionException(
                new RuntimeException("error 1"), new BulkSimpleStatement<>(record1, "INSERT 1")));
    result2 =
        new DefaultReadResult(
            new BulkExecutionException(
                new RuntimeException("error 2"), new BulkSimpleStatement<>(record2, "INSERT 2")));
    result3 =
        new DefaultReadResult(
            new BulkExecutionException(
                new RuntimeException("error 3"), new BulkSimpleStatement<>(record3, "INSERT 3")));
    BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
    batch.add(new BulkSimpleStatement<>(record1, "INSERT 1", "foo", 42));
    batch.add(new BulkSimpleStatement<>(record2, "INSERT 2", "bar", 43));
    batch.add(new BulkSimpleStatement<>(record3, "INSERT 3", "qix", 44));
    batchResult =
        new DefaultReadResult(
            new BulkExecutionException(new RuntimeException("error batch"), batch));
  }

  @Test
  public void should_stop_when_max_extract_errors_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test1");
    LogManager logManager = new LogManager(outputDir, executor, 2, 0, 0, formatter, EXTENDED);
    logManager.init(cluster);
    Flowable<Record> records = Flowable.fromArray(record1, record2, record3);
    try {
      records.compose(logManager.newExtractErrorHandler()).blockingSubscribe();
      fail("Expecting TooManyExtractErrorsException to be thrown");
    } catch (TooManyExtractErrorsException e) {
      assertThat(e).hasMessage("Too many extract errors, the maximum allowed is 2");
      assertThat(e.getMaxExtractErrors()).isEqualTo(2);
    }
    logManager.close();
    Path actual1 =
        logManager
            .getOperationDirectory()
            .resolve(
                String.format("extract-%s.bad", Paths.get(location1.getPath()).toFile().getName()));
    Path actual2 =
        logManager
            .getOperationDirectory()
            .resolve(
                String.format("extract-%s.bad", Paths.get(location2.getPath()).toFile().getName()));
    assertThat(actual1.toFile()).exists();
    assertThat(actual2.toFile()).exists();
    assertThat(Files.list(logManager.getOperationDirectory()).toArray())
        .containsOnly(actual1, actual2);
    List<String> lines1 = Files.readAllLines(actual1, Charset.forName("UTF-8"));
    assertThat(lines1.get(0)).isEqualTo("Location: " + location1);
    assertThat(lines1.get(1)).isEqualTo("Source  : " + LogUtils.formatSingleLine(source1));
    assertThat(lines1.get(2)).isEqualTo("java.lang.RuntimeException: error 1");
    List<String> lines2 = Files.readAllLines(actual2, Charset.forName("UTF-8"));
    assertThat(lines2.get(0)).isEqualTo("Location: " + location2);
    assertThat(lines2.get(1)).isEqualTo("Source  : " + LogUtils.formatSingleLine(source2));
    assertThat(lines2.get(2)).isEqualTo("java.lang.RuntimeException: error 2");
  }

  @Test
  public void should_stop_when_max_transform_errors_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test2");
    LogManager logManager = new LogManager(outputDir, executor, 0, 2, 0, formatter, EXTENDED);
    logManager.init(cluster);
    Flowable<Statement> stmts = Flowable.fromArray(stmt1, stmt2, stmt3);
    try {
      stmts.compose(logManager.newTransformErrorHandler()).blockingSubscribe();
      fail("Expecting TooManyTransformErrorsException to be thrown");
    } catch (TooManyTransformErrorsException e) {
      assertThat(e).hasMessage("Too many transform errors, the maximum allowed is 2");
      assertThat(e.getMaxTransformErrors()).isEqualTo(2);
    }
    logManager.close();
    Path actual = logManager.getOperationDirectory().resolve("transform.bad");
    assertThat(actual.toFile()).exists();
    assertThat(Files.list(logManager.getOperationDirectory()).toArray()).containsOnly(actual);
    List<String> lines = Files.readAllLines(actual, Charset.forName("UTF-8"));
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
  public void should_stop_when_max_load_errors_reached() throws Exception {
    Path outputDir = Files.createTempDirectory("test3");
    LogManager logManager = new LogManager(outputDir, executor, 0, 0, 2, formatter, EXTENDED);
    logManager.init(cluster);
    Flowable<Result> stmts = Flowable.fromArray(result1, result2, result3);
    try {
      stmts.compose(logManager.newLoadErrorHandler()).blockingSubscribe();
      fail("Expecting TooManyLoadErrorsException to be thrown");
    } catch (TooManyLoadErrorsException e) {
      assertThat(e).hasMessage("Too many load errors, the maximum allowed is 2");
      assertThat(e.getMaxLoadErrors()).isEqualTo(2);
    }
    logManager.close();
    Path actual = logManager.getOperationDirectory().resolve("load.bad");
    assertThat(actual.toFile()).exists();
    assertThat(Files.list(logManager.getOperationDirectory()).toArray()).containsOnly(actual);
    List<String> lines = Files.readAllLines(actual, Charset.forName("UTF-8"));
    String content = String.join("\n", lines);
    assertThat(content)
        .containsOnlyOnce("Location: " + location1)
        .containsOnlyOnce("Source  : " + LogUtils.formatSingleLine(source1))
        .contains("INSERT 1")
        .containsOnlyOnce(
            "com.datastax.loader.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 1 (error 1)")
        .containsOnlyOnce("Location: " + location2)
        .containsOnlyOnce("Source  : " + LogUtils.formatSingleLine(source2))
        .contains("INSERT 2")
        .containsOnlyOnce(
            "com.datastax.loader.executor.api.exception.BulkExecutionException: Statement execution failed: INSERT 2 (error 2)");
  }

  @Test
  public void should_stop_when_max_load_errors_reached_and_statements_batched() throws Exception {
    Path outputDir = Files.createTempDirectory("test4");
    LogManager logManager = new LogManager(outputDir, executor, 0, 0, 1, formatter, EXTENDED);
    logManager.init(cluster);
    Flowable<Result> stmts = Flowable.fromArray(batchResult, result1);
    try {
      stmts.compose(logManager.newLoadErrorHandler()).blockingSubscribe();
      fail("Expecting TooManyLoadErrorsException to be thrown");
    } catch (TooManyLoadErrorsException e) {
      assertThat(e).hasMessage("Too many load errors, the maximum allowed is 1");
      assertThat(e.getMaxLoadErrors()).isEqualTo(1);
    }
    logManager.close();
    Path actual = logManager.getOperationDirectory().resolve("load.bad");
    assertThat(actual.toFile()).exists();
    assertThat(Files.list(logManager.getOperationDirectory()).toArray()).containsOnly(actual);
    List<String> lines = Files.readAllLines(actual, Charset.forName("UTF-8"));
    String content = String.join("\n", lines);
    assertThat(content)
        .containsOnlyOnce("Location: " + location1.toExternalForm())
        .containsOnlyOnce("Location: " + location2.toExternalForm())
        .containsOnlyOnce("Location: " + location3.toExternalForm())
        .containsOnlyOnce("Source  : " + LogUtils.formatSingleLine(source1))
        .containsOnlyOnce("Source  : " + LogUtils.formatSingleLine(source2))
        .containsOnlyOnce("Source  : " + LogUtils.formatSingleLine(source3))
        .contains("INSERT 1")
        .contains("INSERT 2")
        .contains("INSERT 3")
        .containsOnlyOnce(
            "com.datastax.loader.executor.api.exception.BulkExecutionException: Statement execution failed")
        .contains("error batch");
  }
}
