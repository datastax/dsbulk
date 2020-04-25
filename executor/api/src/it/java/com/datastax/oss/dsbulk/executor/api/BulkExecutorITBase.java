/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.executor.api;

import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.MockitoAnnotations.initMocks;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.dsbulk.executor.api.result.Result;
import com.datastax.oss.dsbulk.executor.api.result.WriteResult;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;

public abstract class BulkExecutorITBase {

  protected static final String WRITE_QUERY = "INSERT INTO test_write (pk, v) VALUES (%d, %d)";
  private static final SimpleStatement WRITE_STATEMENT =
      SimpleStatement.newInstance(String.format(WRITE_QUERY, 0, 0));

  protected static final String READ_QUERY = "SELECT * FROM test_read";
  private static final SimpleStatement READ_STATEMENT = SimpleStatement.newInstance(READ_QUERY);

  protected static final String FAILED_QUERY = "should fail";
  private static final SimpleStatement FAILED_STATEMENT = SimpleStatement.newInstance(FAILED_QUERY);

  private static final Flux<String> WRITE_QUERIES =
      Flux.range(0, 100).map(i -> String.format(WRITE_QUERY, i, i));

  private static final Flux<String> WRITE_QUERIES_WITH_LAST_BAD =
      WRITE_QUERIES.skipLast(1).concatWith(Flux.just(FAILED_QUERY));

  private static final Flux<SimpleStatement> WRITE_STATEMENTS =
      WRITE_QUERIES.map(SimpleStatement::newInstance);

  private static final Flux<SimpleStatement> WRITE_STATEMENTS_WITH_LAST_BAD =
      WRITE_STATEMENTS.skipLast(1).concatWith(Flux.just(FAILED_STATEMENT));

  protected final BulkExecutor failFastExecutor;
  protected final BulkExecutor failSafeExecutor;

  @Mock private Consumer<? super WriteResult> writeConsumer;
  @Mock private Consumer<? super ReadResult> readConsumer;

  protected BulkExecutorITBase(BulkExecutor failFastExecutor, BulkExecutor failSafeExecutor) {
    this.failFastExecutor = failFastExecutor;
    this.failSafeExecutor = failSafeExecutor;
  }

  @BeforeEach
  void resetMocks() {
    initMocks(this);
  }

  // Tests for synchronous write methods

  @Test
  void writeSyncStringTest() {
    String query = WRITE_QUERIES.blockFirst();
    WriteResult r = failFastExecutor.writeSync(query);
    verifySuccessfulWriteResult(r, query);
  }

  @Test
  void writeSyncStringFailFastTest() {
    try {
      failFastExecutor.writeSync(FAILED_QUERY);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
    }
  }

  @Test
  void writeSyncStringFailSafeTest() {
    WriteResult r = failSafeExecutor.writeSync(FAILED_QUERY);
    verifyFailedWriteResult(r);
  }

  @Test
  void writeSyncStatementTest() {
    WriteResult r = failFastExecutor.writeSync(WRITE_STATEMENT);
    verifySuccessfulWriteResult(r, String.format(WRITE_QUERY, 0, 0));
  }

  @Test
  void writeSyncStatementFailFastTest() {
    try {
      failFastExecutor.writeSync(FAILED_STATEMENT);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
    }
  }

  @Test
  void writeSyncStatementFailSafeTest() {
    WriteResult r = failSafeExecutor.writeSync(FAILED_STATEMENT);
    verifyFailedWriteResult(r);
  }

  @Test
  void writeSyncStreamTest() {
    Stream<SimpleStatement> records = WRITE_STATEMENTS.toStream();
    failFastExecutor.writeSync(records);
    verifyWrites(100);
  }

  @Test
  void writeSyncStreamFailFastTest() {
    try {
      Stream<SimpleStatement> records = WRITE_STATEMENTS_WITH_LAST_BAD.toStream();
      failFastExecutor.writeSync(records);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(99);
    }
  }

  @Test
  void writeSyncStreamFailSafeTest() {
    Stream<SimpleStatement> records = WRITE_STATEMENTS_WITH_LAST_BAD.toStream();
    failSafeExecutor.writeSync(records);
    verifyWrites(99);
  }

  @Test
  void writeSyncStreamConsumerTest() {
    Stream<SimpleStatement> records = WRITE_STATEMENTS.toStream();
    failFastExecutor.writeSync(records, writeConsumer);
    verifyWrites(100);
    verifyWriteConsumer(100, 0);
  }

  @Test
  void writeSyncStreamConsumerFailFastTest() {
    try {
      failFastExecutor.writeSync(Stream.of(FAILED_STATEMENT), writeConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(0);
      verifyWriteConsumer(0, 0);
    }
  }

  @Test
  void writeSyncStreamConsumerFailSafeTest() {
    Stream<SimpleStatement> records = WRITE_STATEMENTS_WITH_LAST_BAD.toStream();
    failSafeExecutor.writeSync(records, writeConsumer);
    verifyWrites(99);
    verifyWriteConsumer(99, 1);
  }

  @Test
  void writeSyncIterableTest() {
    Iterable<SimpleStatement> records = WRITE_STATEMENTS.toIterable();
    failFastExecutor.writeSync(records);
    verifyWrites(100);
  }

  @Test
  void writeSyncIterableFailFastTest() {
    try {
      Iterable<SimpleStatement> records = WRITE_STATEMENTS_WITH_LAST_BAD.toIterable();
      failFastExecutor.writeSync(records);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(99);
    }
  }

  @Test
  void writeSyncIterableFailSafeTest() {
    Iterable<SimpleStatement> records = WRITE_STATEMENTS_WITH_LAST_BAD.toIterable();
    failSafeExecutor.writeSync(records);
    verifyWrites(99);
  }

  @Test
  void writeSyncIterableConsumer() {
    Iterable<SimpleStatement> records = WRITE_STATEMENTS.toIterable();
    failFastExecutor.writeSync(records, writeConsumer);
    verifyWrites(100);
    verifyWriteConsumer(100, 0);
  }

  @Test
  void writeSyncIterableConsumerFailFastTest() {
    try {
      failFastExecutor.writeSync(Collections.singleton(FAILED_STATEMENT), writeConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(0);
      verifyWriteConsumer(0, 0);
    }
  }

  @Test
  void writeSyncIterableConsumerFailSafeTest() {
    Iterable<SimpleStatement> records = WRITE_STATEMENTS_WITH_LAST_BAD.toIterable();
    failSafeExecutor.writeSync(records, writeConsumer);
    verifyWrites(99);
    verifyWriteConsumer(99, 1);
  }

  @Test
  void writeSyncPublisherTest() {
    failFastExecutor.writeSync(WRITE_STATEMENTS);
    verifyWrites(100);
  }

  @Test
  void writeSyncPublisherFailFastTest() {
    try {
      failFastExecutor.writeSync(WRITE_STATEMENTS_WITH_LAST_BAD);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(99);
    }
  }

  @Test
  void writeSyncPublisherFailSafeTest() {
    failSafeExecutor.writeSync(WRITE_STATEMENTS_WITH_LAST_BAD);
    verifyWrites(99);
  }

  @Test
  void writeSyncPublisherConsumer() {
    failFastExecutor.writeSync(WRITE_STATEMENTS, writeConsumer);
    verifyWrites(100);
    verifyWriteConsumer(100, 0);
  }

  @Test
  void writeSyncPublisherConsumerFailFastTest() {
    try {
      failFastExecutor.writeSync(Flux.just(FAILED_STATEMENT), writeConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(0);
      verifyWriteConsumer(0, 0);
    }
  }

  @Test
  void writeSyncPublisherConsumerFailSafeTest() {
    failSafeExecutor.writeSync(WRITE_STATEMENTS_WITH_LAST_BAD, writeConsumer);
    verifyWrites(99);
    verifyWriteConsumer(99, 1);
  }

  // Tests for asynchronous write methods

  @Test
  void writeAsyncStringTest() throws Exception {
    String query = WRITE_QUERIES.blockFirst();
    WriteResult r = failFastExecutor.writeAsync(query).get();
    verifySuccessfulWriteResult(r, query);
  }

  @Test
  void writeAsyncStringFailFastTest() throws Exception {
    try {
      failFastExecutor.writeAsync(FAILED_QUERY).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
    }
  }

  @Test
  void writeAsyncStringFailSafeTest() throws Exception {
    WriteResult r = failSafeExecutor.writeAsync(FAILED_QUERY).get();
    verifyFailedWriteResult(r);
  }

  @Test
  void writeAsyncStatementTest() throws Exception {
    WriteResult r = failFastExecutor.writeAsync(WRITE_STATEMENT).get();
    verifySuccessfulWriteResult(r, String.format(WRITE_QUERY, 0, 0));
  }

  @Test
  void writeAsyncStatementFailFastTest() throws Exception {
    try {
      failFastExecutor.writeAsync(FAILED_STATEMENT).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
    }
  }

  @Test
  void writeAsyncStatementFailSafeTest() throws Exception {
    WriteResult r = failSafeExecutor.writeAsync(FAILED_STATEMENT).get();
    verifyFailedWriteResult(r);
  }

  @Test
  void writeAsyncStreamTest() throws Exception {
    Stream<SimpleStatement> records = stream(WRITE_STATEMENTS.toIterable().spliterator(), false);
    failFastExecutor.writeAsync(records).get();
    verifyWrites(100);
  }

  @Test
  void writeAsyncStreamFailFastTest() throws Exception {
    try {
      Stream<SimpleStatement> records =
          stream(WRITE_STATEMENTS_WITH_LAST_BAD.toIterable().spliterator(), false);
      failFastExecutor.writeAsync(records).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifyWrites(99);
    }
  }

  @Test
  void writeAsyncStreamFailSafeTest() throws Exception {
    Stream<SimpleStatement> records =
        stream(WRITE_STATEMENTS_WITH_LAST_BAD.toIterable().spliterator(), false);
    failSafeExecutor.writeAsync(records).get();
    verifyWrites(99);
  }

  @Test
  void writeAsyncStreamConsumerTest() throws Exception {
    Stream<SimpleStatement> records = stream(WRITE_STATEMENTS.toIterable().spliterator(), false);
    failFastExecutor.writeAsync(records, writeConsumer).get();
    verifyWrites(100);
    verifyWriteConsumer(100, 0);
  }

  @Test
  void writeAsyncStreamConsumerFailFastTest() throws Exception {
    try {
      failFastExecutor.writeAsync(Stream.of(FAILED_STATEMENT), writeConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifyWrites(0);
      verifyWriteConsumer(0, 0);
    }
  }

  @Test
  void writeAsyncStreamConsumerFailSafeTest() throws Exception {
    Stream<SimpleStatement> records =
        stream(WRITE_STATEMENTS_WITH_LAST_BAD.toIterable().spliterator(), false);
    failSafeExecutor.writeAsync(records, writeConsumer).get();
    verifyWrites(99);
    verifyWriteConsumer(99, 1);
  }

  @Test
  void writeAsyncIterableTest() throws Exception {
    Iterable<SimpleStatement> records = WRITE_STATEMENTS.toIterable();
    failFastExecutor.writeAsync(records).get();
    verifyWrites(100);
  }

  @Test
  void writeAsyncIterableFailFastTest() throws Exception {
    try {
      Iterable<SimpleStatement> records = WRITE_STATEMENTS_WITH_LAST_BAD.toIterable();
      failFastExecutor.writeAsync(records).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifyWrites(99);
    }
  }

  @Test
  void writeAsyncIterableFailSafeTest() throws Exception {
    Iterable<SimpleStatement> records = WRITE_STATEMENTS_WITH_LAST_BAD.toIterable();
    failSafeExecutor.writeAsync(records).get();
    verifyWrites(99);
  }

  @Test
  void writeAsyncIterableConsumer() throws Exception {
    Iterable<SimpleStatement> records = WRITE_STATEMENTS.toIterable();
    failFastExecutor.writeAsync(records, writeConsumer).get();
    verifyWrites(100);
    verifyWriteConsumer(100, 0);
  }

  @Test
  void writeAsyncIterableConsumerFailFastTest() throws Exception {
    try {
      failFastExecutor.writeAsync(Collections.singleton(FAILED_STATEMENT), writeConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifyWrites(0);
      verifyWriteConsumer(0, 0);
    }
  }

  @Test
  void writeAsyncIterableConsumerFailSafeTest() throws Exception {
    Iterable<SimpleStatement> records = WRITE_STATEMENTS_WITH_LAST_BAD.toIterable();
    failSafeExecutor.writeAsync(records, writeConsumer).get();
    verifyWrites(99);
    verifyWriteConsumer(99, 1);
  }

  @Test
  void writeAsyncPublisherTest() throws Exception {
    failFastExecutor.writeAsync(WRITE_STATEMENTS).get();
    verifyWrites(100);
  }

  @Test
  void writeAsyncPublisherFailFastTest() throws Exception {
    try {
      failFastExecutor.writeAsync(WRITE_STATEMENTS_WITH_LAST_BAD).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifyWrites(99);
    }
  }

  @Test
  void writeAsyncPublisherFailSafeTest() throws Exception {
    failSafeExecutor.writeAsync(WRITE_STATEMENTS_WITH_LAST_BAD).get();
    verifyWrites(99);
  }

  @Test
  void writeAsyncPublisherConsumer() throws Exception {
    failFastExecutor.writeAsync(WRITE_STATEMENTS, writeConsumer).get();
    verifyWrites(100);
    verifyWriteConsumer(100, 0);
  }

  @Test
  void writeAsyncPublisherConsumerFailFastTest() throws Exception {
    try {
      failFastExecutor.writeAsync(Flux.just(FAILED_STATEMENT), writeConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifyWrites(0);
      verifyWriteConsumer(0, 0);
    }
  }

  @Test
  void writeAsyncPublisherConsumerFailSafeTest() throws Exception {
    failSafeExecutor.writeAsync(WRITE_STATEMENTS_WITH_LAST_BAD, writeConsumer).get();
    verifyWrites(99);
    verifyWriteConsumer(99, 1);
  }

  // Tests for rx write methods

  @Test
  void writeReactiveStringTest() {
    WRITE_QUERIES.flatMap(failFastExecutor::writeReactive).blockLast();
    verifyWrites(100);
  }

  @Test
  void writeReactiveStringFailFastTest() {
    try {
      WRITE_QUERIES_WITH_LAST_BAD.flatMap(failFastExecutor::writeReactive).blockLast();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(99);
    }
  }

  @Test
  void writeReactiveStringFailSafeTest() {
    Flux.just(FAILED_QUERY).flatMap(failSafeExecutor::writeReactive).blockLast();
    verifyWrites(0);
  }

  @Test
  void writeReactiveStatementTest() {
    WRITE_STATEMENTS.flatMap(failFastExecutor::writeReactive).blockLast();
    verifyWrites(100);
  }

  @Test
  void writeReactiveStatementFailFastTest() {
    try {
      WRITE_STATEMENTS_WITH_LAST_BAD.flatMap(failFastExecutor::writeReactive).blockLast();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(99);
    }
  }

  @Test
  void writeReactiveStatementFailSafeTest() {
    Flux.just(FAILED_STATEMENT).flatMap(failSafeExecutor::writeReactive).blockLast();
    verifyWrites(0);
  }

  @Test
  void writeReactiveStreamTest() {
    Stream<SimpleStatement> statements = stream(WRITE_STATEMENTS.toIterable().spliterator(), false);
    Flux.from(failFastExecutor.writeReactive(statements)).blockLast();
    verifyWrites(100);
  }

  @Test
  void writeReactiveStreamFailFastTest() {
    try {
      Stream<SimpleStatement> statements =
          stream(WRITE_STATEMENTS_WITH_LAST_BAD.toIterable().spliterator(), false);
      Flux.from(failFastExecutor.writeReactive(statements)).blockLast();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(99);
    }
  }

  @Test
  void writeReactiveStreamFailSafeTest() {
    Stream<SimpleStatement> statements =
        stream(WRITE_STATEMENTS_WITH_LAST_BAD.toIterable().spliterator(), false);
    Flux.from(failSafeExecutor.writeReactive(statements)).blockLast();
    verifyWrites(99);
  }

  @Test
  void writeReactiveIterableTest() {
    Iterable<SimpleStatement> statements = WRITE_STATEMENTS.toIterable();
    Flux.from(failFastExecutor.writeReactive(statements)).blockLast();
    verifyWrites(100);
  }

  @Test
  void writeReactiveIterableFailFastTest() {
    try {
      Iterable<SimpleStatement> statements = WRITE_STATEMENTS_WITH_LAST_BAD.toIterable();
      Flux.from(failFastExecutor.writeReactive(statements)).blockLast();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(99);
    }
  }

  @Test
  void writeReactiveIterableFailSafeTest() {
    Iterable<SimpleStatement> statements = WRITE_STATEMENTS_WITH_LAST_BAD.toIterable();
    Flux.from(failSafeExecutor.writeReactive(statements)).blockLast();
    verifyWrites(99);
  }

  @Test
  void writeReactivePublisherTest() {
    Flux.from(failFastExecutor.writeReactive(WRITE_STATEMENTS)).blockLast();
    verifyWrites(100);
  }

  @Test
  void writeReactivePublisherFailFastTest() {
    try {
      Flux.from(failFastExecutor.writeReactive(WRITE_STATEMENTS_WITH_LAST_BAD)).blockLast();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(99);
    }
  }

  @Test
  void writeReactivePublisherFailSafeTest() {
    Flux.from(failSafeExecutor.writeReactive(WRITE_STATEMENTS_WITH_LAST_BAD)).blockLast();
    verifyWrites(99);
  }

  // Tests for synchronous read methods

  @Test
  void readSyncStringConsumerTest() {
    failFastExecutor.readSync(READ_QUERY, readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(100, 0);
    verifyReads(100, 0, readResults);
  }

  @Test
  void readSyncStringConsumerFailFastTest() {
    try {
      failFastExecutor.readSync(FAILED_QUERY, readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readSyncStringConsumerFailSafeTest() {
    failSafeExecutor.readSync(FAILED_QUERY, readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(0, 1);
    verifyReads(0, 1, readResults);
  }

  @Test
  void readSyncStatementConsumerTest() {
    failFastExecutor.readSync(READ_STATEMENT, readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(100, 0);
    verifyReads(100, 0, readResults);
  }

  @Test
  void readSyncStatementConsumerFailFastTest() {
    try {
      failFastExecutor.readSync(FAILED_STATEMENT, readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readSyncStatementConsumerFailSafeTest() {
    failSafeExecutor.readSync(FAILED_STATEMENT, readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(0, 1);
    verifyReads(0, 1, readResults);
  }

  @Test
  void readSyncStreamConsumerTest() {
    failFastExecutor.readSync(Stream.of(READ_STATEMENT), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(100, 0);
    verifyReads(100, 0, readResults);
  }

  @Test
  void readSyncStreamConsumerFailFastTest() {
    try {
      failFastExecutor.readSync(Stream.of(FAILED_STATEMENT), readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readSyncStreamConsumerFailSafeTest() {
    failSafeExecutor.readSync(Stream.of(READ_STATEMENT, FAILED_STATEMENT), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(100, 1);
    verifyReads(100, 1, readResults);
  }

  @Test
  void readSyncIterableConsumer() {
    failFastExecutor.readSync(Collections.singleton(READ_STATEMENT), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(100, 0);
    verifyReads(100, 0, readResults);
  }

  @Test
  void readSyncIterableConsumerFailFastTest() {
    try {
      failFastExecutor.readSync(Collections.singleton(FAILED_STATEMENT), readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readSyncIterableConsumerFailSafeTest() {
    failSafeExecutor.readSync(Arrays.asList(READ_STATEMENT, FAILED_STATEMENT), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(100, 1);
    verifyReads(100, 1, readResults);
  }

  @Test
  void readSyncPublisherConsumer() {
    failFastExecutor.readSync(Flux.just(READ_STATEMENT), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(100, 0);
    verifyReads(100, 0, readResults);
  }

  @Test
  void readSyncPublisherConsumerFailFastTest() {
    try {
      failFastExecutor.readSync(Flux.just(FAILED_STATEMENT), readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readSyncPublisherConsumerFailSafeTest() {
    failSafeExecutor.readSync(Flux.just(READ_STATEMENT, FAILED_STATEMENT), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(100, 1);
    verifyReads(100, 1, readResults);
  }

  // Tests for asynchronous read methods

  @Test
  void readAsyncStringConsumerTest() throws Exception {
    failSafeExecutor.readAsync(READ_QUERY, readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(100, 0);
    verifyReads(100, 0, readResults);
  }

  @Test
  void readAsyncStringConsumerFailFastTest() throws Exception {
    try {
      failFastExecutor.readAsync(FAILED_QUERY, readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readAsyncStringConsumerFailSafeTest() throws Exception {
    failSafeExecutor.readAsync(FAILED_QUERY, readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(0, 1);
    verifyReads(0, 1, readResults);
  }

  @Test
  void readAsyncStatementConsumerTest() throws Exception {
    failSafeExecutor.readAsync(READ_STATEMENT, readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(100, 0);
    verifyReads(100, 0, readResults);
  }

  @Test
  void readAsyncStatementConsumerFailFastTest() throws Exception {
    try {
      failFastExecutor.readAsync(FAILED_STATEMENT, readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readAsyncStatementConsumerFailSafeTest() throws Exception {
    failSafeExecutor.readAsync(FAILED_STATEMENT, readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(0, 1);
    verifyReads(0, 1, readResults);
  }

  @Test
  void readAsyncStreamConsumerTest() throws Exception {
    failSafeExecutor.readAsync(Stream.of(READ_STATEMENT), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(100, 0);
    verifyReads(100, 0, readResults);
  }

  @Test
  void readAsyncStreamConsumerFailFastTest() throws Exception {
    try {
      failFastExecutor.readAsync(Stream.of(FAILED_STATEMENT), readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readAsyncStreamConsumerFailSafeTest() throws Exception {
    failSafeExecutor.readAsync(Stream.of(READ_STATEMENT, FAILED_STATEMENT), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(100, 1);
    verifyReads(100, 1, readResults);
  }

  @Test
  void readAsyncIterableConsumer() throws Exception {
    failSafeExecutor.readAsync(Collections.singleton(READ_STATEMENT), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(100, 0);
    verifyReads(100, 0, readResults);
  }

  @Test
  void readAsyncIterableConsumerFailFastTest() throws Exception {
    try {
      failFastExecutor.readAsync(Collections.singleton(FAILED_STATEMENT), readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readAsyncIterableConsumerFailSafeTest() throws Exception {
    failSafeExecutor.readAsync(Arrays.asList(READ_STATEMENT, FAILED_STATEMENT), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(100, 1);
    verifyReads(100, 1, readResults);
  }

  @Test
  void readAsyncPublisherConsumer() throws Exception {
    failSafeExecutor.readAsync(Flux.just(READ_STATEMENT), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(100, 0);
    verifyReads(100, 0, readResults);
  }

  @Test
  void readAsyncPublisherConsumerFailFastTest() throws Exception {
    try {
      failFastExecutor.readAsync(Flux.just(FAILED_STATEMENT), readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readAsyncPublisherConsumerFailSafeTest() throws Exception {
    failSafeExecutor.readAsync(Flux.just(READ_STATEMENT, FAILED_STATEMENT), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(100, 1);
    verifyReads(100, 1, readResults);
  }

  // Tests for rx read methods

  @Test
  void readReactiveStringTest() {
    Iterable<ReadResult> readResults =
        Flux.just(READ_QUERY).flatMap(failFastExecutor::readReactive).toIterable();
    verifyReads(100, 0, readResults);
  }

  @Test
  void readReactiveStringFailFastTest() {
    Iterable<ReadResult> readResults = Collections.emptyList();
    try {
      Flux.just(FAILED_QUERY).flatMap(failFastExecutor::readReactive).blockLast();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readReactiveStringFailSafeTest() {
    Iterable<ReadResult> readResults =
        Flux.just(FAILED_QUERY).flatMap(failSafeExecutor::readReactive).toIterable();
    verifyReads(0, 1, readResults);
  }

  @Test
  void readReactiveStatementTest() {
    Iterable<ReadResult> readResults =
        Flux.just(READ_STATEMENT).flatMap(failFastExecutor::readReactive).toIterable();
    verifyReads(100, 0, readResults);
  }

  @Test
  void readReactiveStatementFailFastTest() {
    Iterable<ReadResult> readResults = Collections.emptyList();
    try {
      Flux.just(FAILED_STATEMENT).flatMap(failFastExecutor::readReactive).blockLast();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readReactiveStatementFailSafeTest() {
    Iterable<ReadResult> readResults =
        Flux.just(FAILED_STATEMENT).flatMap(failSafeExecutor::readReactive).toIterable();
    verifyReads(0, 1, readResults);
  }

  @Test
  void readReactiveStreamTest() {
    Queue<ReadResult> readResults = new ConcurrentLinkedQueue<>();
    Flux.from(failFastExecutor.readReactive(Stream.of(READ_STATEMENT)))
        .doOnNext(readResults::add)
        .blockLast();
    verifyReads(100, 0, readResults);
  }

  @Test
  void readReactiveStreamFailFastTest() {
    Queue<ReadResult> readResults = new ConcurrentLinkedQueue<>();
    try {
      Flux.from(failFastExecutor.readReactive(Stream.of(FAILED_STATEMENT)))
          .doOnNext(readResults::add)
          .blockLast();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readReactiveStreamFailSafeTest() {
    Queue<ReadResult> readResults = new ConcurrentLinkedQueue<>();
    Flux.from(failSafeExecutor.readReactive(Stream.of(READ_STATEMENT, FAILED_STATEMENT)))
        .doOnNext(readResults::add)
        .blockLast();
    verifyReads(100, 1, readResults);
  }

  @Test
  void readReactiveIterableTest() {
    Iterable<ReadResult> readResults =
        Flux.from(failFastExecutor.readReactive(Collections.singleton(READ_STATEMENT)))
            .toIterable();
    verifyReads(100, 0, readResults);
  }

  @Test
  void readReactiveIterableFailFastTest() {
    Queue<ReadResult> readResults = new ConcurrentLinkedQueue<>();
    try {
      Flux.from(failFastExecutor.readReactive(Collections.singleton(FAILED_STATEMENT)))
          .doOnNext(readResults::add)
          .blockLast();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readReactiveIterableFailSafeTest() {
    Iterable<ReadResult> readResults =
        Flux.from(failSafeExecutor.readReactive(Arrays.asList(READ_STATEMENT, FAILED_STATEMENT)))
            .toIterable();
    verifyReads(100, 1, readResults);
  }

  @Test
  void readReactivePublisherTest() {
    Iterable<ReadResult> readResults =
        Flux.from(failFastExecutor.readReactive(Flux.just(READ_STATEMENT))).toIterable();
    verifyReads(100, 0, readResults);
  }

  @Test
  void readReactivePublisherFailFastTest() {
    Queue<ReadResult> readResults = new ConcurrentLinkedQueue<>();
    try {
      Flux.from(failFastExecutor.readReactive(Flux.just(FAILED_STATEMENT)))
          .doOnNext(readResults::add)
          .blockLast();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readReactivePublisherFailSafeTest() {
    Iterable<ReadResult> readResults =
        Flux.from(failSafeExecutor.readReactive(Flux.just(READ_STATEMENT, FAILED_STATEMENT)))
            .toIterable();
    verifyReads(100, 1, readResults);
  }

  protected abstract void verifyWrites(int expected);

  protected void verifyReads(
      int expectedSuccessful, int expectedFailed, Iterable<ReadResult> actual) {
    AtomicInteger i = new AtomicInteger();
    long actualSuccessful =
        Flux.fromIterable(actual)
            .filter(Result::isSuccess)
            .map(result -> result.getRow().orElseThrow(AssertionError::new))
            .map(row -> row.getInt("pk"))
            .sort()
            .doOnNext(
                pk -> {
                  assertThat(pk).isEqualTo(i.get());
                  i.getAndIncrement();
                })
            .count()
            .blockOptional().orElse(0L);
    assertThat(actualSuccessful).isEqualTo(expectedSuccessful);
    long actualFailed =
        Flux.fromIterable(actual)
            .filter(r -> !r.isSuccess())
            .doOnNext(
                r -> {
                  assertThat(r.getRow().isPresent()).isFalse();
                  assertThat(r.getError().isPresent()).isTrue();
                  BulkExecutionException error = r.getError().get();
                  verifyException(error);
                })
            .count()
            .blockOptional()
            .orElse(0L);
    assertThat(actualFailed).isEqualTo(expectedFailed);
  }

  private void verifySuccessfulWriteResult(WriteResult r, String expected) {
    assertThat(r.isSuccess()).isTrue();
    assertThat(((SimpleStatement) r.getStatement()).getQuery()).isEqualTo(expected);
    assertThat(r.getExecutionInfo().isPresent()).isTrue();
  }

  private void verifyFailedWriteResult(WriteResult r) {
    assertThat(r.isSuccess()).isFalse();
    assertThat(((SimpleStatement) r.getStatement()).getQuery()).isEqualTo(FAILED_QUERY);
    assertThat(r.getExecutionInfo().isPresent()).isFalse();
  }

  private void verifyException(Throwable t) {
    assertThat(t).hasCauseExactlyInstanceOf(SyntaxError.class);
  }

  private void verifyWriteConsumer(int expectedSuccessful, int expectedFailed) {
    ArgumentCaptor<WriteResult> captor = ArgumentCaptor.forClass(WriteResult.class);
    Mockito.verify(writeConsumer, Mockito.times(expectedSuccessful + expectedFailed))
        .accept(captor.capture());
    List<WriteResult> values = captor.getAllValues();
    assertThat(values.stream().filter(Result::isSuccess).count()).isEqualTo(expectedSuccessful);
    assertThat(values.stream().filter(r -> !r.isSuccess()).count()).isEqualTo(expectedFailed);
    values.stream()
        .filter(Result::isSuccess)
        .forEach(
            r -> {
              assertThat(r.getError().isPresent()).isFalse();
              assertThat(r.getExecutionInfo().isPresent()).isTrue();
            });
    values.stream()
        .filter(r -> !r.isSuccess())
        .forEach(
            r -> {
              assertThat(r.getError().isPresent()).isTrue();
              assertThat(r.getExecutionInfo().isPresent()).isFalse();
            });
  }

  private List<ReadResult> verifyReadConsumer(int expectedSuccessful, int expectedFailed) {
    ArgumentCaptor<ReadResult> captor = ArgumentCaptor.forClass(ReadResult.class);
    Mockito.verify(readConsumer, Mockito.times(expectedSuccessful + expectedFailed))
        .accept(captor.capture());
    List<ReadResult> values = captor.getAllValues();
    assertThat(values.stream().filter(Result::isSuccess).count()).isEqualTo(expectedSuccessful);
    assertThat(values.stream().filter(r -> !r.isSuccess()).count()).isEqualTo(expectedFailed);
    values.stream()
        .filter(Result::isSuccess)
        .forEach(
            r -> {
              assertThat(r.getError().isPresent()).isFalse();
              assertThat(r.getRow().isPresent()).isTrue();
            });
    values.stream()
        .filter(r -> !r.isSuccess())
        .forEach(
            r -> {
              assertThat(r.getError().isPresent()).isTrue();
              assertThat(r.getRow().isPresent()).isFalse();
            });
    return values;
  }
}
