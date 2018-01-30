/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.ccm;

import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.createIpByCountryTable;
import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.prepareInsertStatement;
import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.truncateIpByCountryTable;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.MockitoAnnotations.initMocks;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.dsbulk.commons.tests.ccm.CCMExtension;
import com.datastax.dsbulk.commons.tests.utils.CsvUtils;
import com.datastax.dsbulk.executor.api.BulkExecutor;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dsbulk.executor.api.result.Result;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.univocity.parsers.common.record.Record;
import io.reactivex.Flowable;
import io.reactivex.plugins.RxJavaPlugins;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

@SuppressWarnings("Duplicates")
@ExtendWith(CCMExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BulkExecutorCCMITBase {

  private static final SimpleStatement READ_SUCCESSFUL =
      new SimpleStatement("SELECT * FROM ip_by_country");
  private static final SimpleStatement FAILED = new SimpleStatement("should fail");
  private static final String FIRST_QUERY = CsvUtils.firstQuery();

  private final Session session;
  private final BulkExecutor failFastExecutor;
  private final BulkExecutor failSafeExecutor;

  private final PreparedStatement insertStatement;

  @Mock private Consumer<? super WriteResult> writeConsumer;

  @Mock private Consumer<? super ReadResult> readConsumer;

  public BulkExecutorCCMITBase(
      Session session, BulkExecutor failFastExecutor, BulkExecutor failSafeExecutor) {
    this.session = session;
    this.failFastExecutor = failFastExecutor;
    this.failSafeExecutor = failSafeExecutor;
    createIpByCountryTable(session);
    insertStatement = prepareInsertStatement(session);
  }

  @BeforeAll
  static void disableStackTraces() {
    RxJavaPlugins.setErrorHandler((t) -> {});
  }

  @BeforeEach
  void resetMocks() throws Exception {
    initMocks(this);
  }

  @AfterEach
  void truncateTable() {
    truncateIpByCountryTable(session);
  }

  // Tests for synchronous write methods

  @Test
  void writeSyncStringTest() {
    WriteResult r = failFastExecutor.writeSync(FIRST_QUERY);
    verifySuccessfulWriteResult(r);
  }

  @Test
  void writeSyncStringFailFastTest() {
    try {
      failFastExecutor.writeSync(FAILED.getQueryString());
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
    }
  }

  @Test
  void writeSyncStringFailSafeTest() {
    WriteResult r = failSafeExecutor.writeSync(FAILED.getQueryString());
    verifyFailedWriteResult(r);
  }

  @Test
  void writeSyncStatementTest() {
    WriteResult r = failFastExecutor.writeSync(FIRST_QUERY);
    verifySuccessfulWriteResult(r);
  }

  @Test
  void writeSyncStatementFailFastTest() {
    try {
      failFastExecutor.writeSync(FAILED);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
    }
  }

  @Test
  void writeSyncStatementFailSafeTest() {
    WriteResult r = failSafeExecutor.writeSync(FAILED);
    verifyFailedWriteResult(r);
  }

  @Test
  void writeSyncStreamTest() {
    Stream<SimpleStatement> records =
        stream(CsvUtils.simpleStatements().blockingIterable().spliterator(), false);
    failFastExecutor.writeSync(records);
    verifyWrites(500);
  }

  @Test
  void writeSyncStreamFailFastTest() {
    try {
      Stream<Statement> records =
          stream(sampleStatementsWithLastBad().blockingIterable().spliterator(), false);
      failFastExecutor.writeSync(records);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(499);
    }
  }

  @Test
  void writeSyncStreamFailSafeTest() {
    Stream<Statement> records =
        stream(sampleStatementsWithLastBad().blockingIterable().spliterator(), false);
    failSafeExecutor.writeSync(records);
    verifyWrites(499);
  }

  @Test
  void writeSyncStreamConsumerTest() {
    Stream<SimpleStatement> records =
        stream(CsvUtils.simpleStatements().blockingIterable().spliterator(), false);
    failFastExecutor.writeSync(records, writeConsumer);
    verifyWrites(500);
    verifyWriteConsumer(500, 0);
  }

  @Test
  void writeSyncStreamConsumerFailFastTest() {
    try {
      failFastExecutor.writeSync(Stream.of(FAILED), writeConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(0);
      verifyWriteConsumer(0, 0);
    }
  }

  @Test
  void writeSyncStreamConsumerFailSafeTest() {
    Stream<Statement> records =
        stream(sampleStatementsWithLastBad().blockingIterable().spliterator(), false);
    failSafeExecutor.writeSync(records, writeConsumer);
    verifyWrites(499);
    verifyWriteConsumer(499, 1);
  }

  @Test
  void writeSyncIterableTest() {
    Iterable<SimpleStatement> records = CsvUtils.simpleStatements().blockingIterable();
    failFastExecutor.writeSync(records);
    verifyWrites(500);
  }

  @Test
  void writeSyncIterableFailFastTest() {
    try {
      Iterable<Statement> records = sampleStatementsWithLastBad().blockingIterable();
      failFastExecutor.writeSync(records);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(499);
    }
  }

  @Test
  void writeSyncIterableFailSafeTest() {
    Iterable<Statement> records = sampleStatementsWithLastBad().blockingIterable();
    failSafeExecutor.writeSync(records);
    verifyWrites(499);
  }

  @Test
  void writeSyncIterableConsumer() {
    Iterable<SimpleStatement> records = CsvUtils.simpleStatements().blockingIterable();
    failFastExecutor.writeSync(records, writeConsumer);
    verifyWrites(500);
    verifyWriteConsumer(500, 0);
  }

  @Test
  void writeSyncIterableConsumerFailFastTest() {
    try {
      failFastExecutor.writeSync(Collections.singleton(FAILED), writeConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(0);
      verifyWriteConsumer(0, 0);
    }
  }

  @Test
  void writeSyncIterableConsumerFailSafeTest() {
    Iterable<Statement> records = sampleStatementsWithLastBad().blockingIterable();
    failSafeExecutor.writeSync(records, writeConsumer);
    verifyWrites(499);
    verifyWriteConsumer(499, 1);
  }

  @Test
  void writeSyncPublisherTest() {
    failFastExecutor.writeSync(CsvUtils.simpleStatements());
    verifyWrites(500);
  }

  @Test
  void writeSyncPublisherFailFastTest() {
    try {
      failFastExecutor.writeSync(sampleStatementsWithLastBad());
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(499);
    }
  }

  @Test
  void writeSyncPublisherFailSafeTest() {
    failSafeExecutor.writeSync(sampleStatementsWithLastBad());
    verifyWrites(499);
  }

  @Test
  void writeSyncPublisherConsumer() {
    failFastExecutor.writeSync(CsvUtils.simpleStatements(), writeConsumer);
    verifyWrites(500);
    verifyWriteConsumer(500, 0);
  }

  @Test
  void writeSyncPublisherConsumerFailFastTest() {
    try {
      failFastExecutor.writeSync(Flowable.just(FAILED), writeConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(0);
      verifyWriteConsumer(0, 0);
    }
  }

  @Test
  void writeSyncPublisherConsumerFailSafeTest() {
    failSafeExecutor.writeSync(sampleStatementsWithLastBad(), writeConsumer);
    verifyWrites(499);
    verifyWriteConsumer(499, 1);
  }

  // Tests for asynchronous write methods

  @Test
  void writeAsyncStringTest() throws Exception {
    WriteResult r = failFastExecutor.writeAsync(CsvUtils.firstQuery()).get();
    verifySuccessfulWriteResult(r);
  }

  @Test
  void writeAsyncStringFailFastTest() throws Exception {
    try {
      failFastExecutor.writeAsync(FAILED.getQueryString()).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
    }
  }

  @Test
  void writeAsyncStringFailSafeTest() throws Exception {
    WriteResult r = failSafeExecutor.writeAsync(FAILED.getQueryString()).get();
    verifyFailedWriteResult(r);
  }

  @Test
  void writeAsyncStatementTest() throws Exception {
    WriteResult r = failFastExecutor.writeAsync(CsvUtils.firstQuery()).get();
    verifySuccessfulWriteResult(r);
  }

  @Test
  void writeAsyncStatementFailFastTest() throws Exception {
    try {
      failFastExecutor.writeAsync(FAILED).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
    }
  }

  @Test
  void writeAsyncStatementFailSafeTest() throws Exception {
    WriteResult r = failSafeExecutor.writeAsync(FAILED).get();
    verifyFailedWriteResult(r);
  }

  @Test
  void writeAsyncStreamTest() throws Exception {
    Stream<SimpleStatement> records =
        stream(CsvUtils.simpleStatements().blockingIterable().spliterator(), false);
    failFastExecutor.writeAsync(records).get();
    verifyWrites(500);
  }

  @Test
  void writeAsyncStreamFailFastTest() throws Exception {
    try {
      Stream<Statement> records =
          stream(sampleStatementsWithLastBad().blockingIterable().spliterator(), false);
      failFastExecutor.writeAsync(records).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifyWrites(499);
    }
  }

  @Test
  void writeAsyncStreamFailSafeTest() throws Exception {
    Stream<Statement> records =
        stream(sampleStatementsWithLastBad().blockingIterable().spliterator(), false);
    failSafeExecutor.writeAsync(records).get();
    verifyWrites(499);
  }

  @Test
  void writeAsyncStreamConsumerTest() throws Exception {
    Stream<SimpleStatement> records =
        stream(CsvUtils.simpleStatements().blockingIterable().spliterator(), false);
    failFastExecutor.writeAsync(records, writeConsumer).get();
    verifyWrites(500);
    verifyWriteConsumer(500, 0);
  }

  @Test
  void writeAsyncStreamConsumerFailFastTest() throws Exception {
    try {
      failFastExecutor.writeAsync(Stream.of(FAILED), writeConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifyWrites(0);
      verifyWriteConsumer(0, 0);
    }
  }

  @Test
  void writeAsyncStreamConsumerFailSafeTest() throws Exception {
    Stream<Statement> records =
        stream(sampleStatementsWithLastBad().blockingIterable().spliterator(), false);
    failSafeExecutor.writeAsync(records, writeConsumer).get();
    verifyWrites(499);
    verifyWriteConsumer(499, 1);
  }

  @Test
  void writeAsyncIterableTest() throws Exception {
    Iterable<SimpleStatement> records = CsvUtils.simpleStatements().blockingIterable();
    failFastExecutor.writeAsync(records).get();
    verifyWrites(500);
  }

  @Test
  void writeAsyncIterableFailFastTest() throws Exception {
    try {
      Iterable<Statement> records = sampleStatementsWithLastBad().blockingIterable();
      failFastExecutor.writeAsync(records).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifyWrites(499);
    }
  }

  @Test
  void writeAsyncIterableFailSafeTest() throws Exception {
    Iterable<Statement> records = sampleStatementsWithLastBad().blockingIterable();
    failSafeExecutor.writeAsync(records).get();
    verifyWrites(499);
  }

  @Test
  void writeAsyncIterableConsumer() throws Exception {
    Iterable<SimpleStatement> records = CsvUtils.simpleStatements().blockingIterable();
    failFastExecutor.writeAsync(records, writeConsumer).get();
    verifyWrites(500);
    verifyWriteConsumer(500, 0);
  }

  @Test
  void writeAsyncIterableConsumerFailFastTest() throws Exception {
    try {
      failFastExecutor.writeAsync(Collections.singleton(FAILED), writeConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifyWrites(0);
      verifyWriteConsumer(0, 0);
    }
  }

  @Test
  void writeAsyncIterableConsumerFailSafeTest() throws Exception {
    Iterable<Statement> records = sampleStatementsWithLastBad().blockingIterable();
    failSafeExecutor.writeAsync(records, writeConsumer).get();
    verifyWrites(499);
    verifyWriteConsumer(499, 1);
  }

  @Test
  void writeAsyncPublisherTest() throws Exception {
    failFastExecutor.writeAsync(CsvUtils.simpleStatements()).get();
    verifyWrites(500);
  }

  @Test
  void writeAsyncPublisherFailFastTest() throws Exception {
    try {
      failFastExecutor.writeAsync(sampleStatementsWithLastBad()).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifyWrites(499);
    }
  }

  @Test
  void writeAsyncPublisherFailSafeTest() throws Exception {
    failSafeExecutor.writeAsync(sampleStatementsWithLastBad()).get();
    verifyWrites(499);
  }

  @Test
  void writeAsyncPublisherConsumer() throws Exception {
    failFastExecutor.writeAsync(CsvUtils.simpleStatements(), writeConsumer).get();
    verifyWrites(500);
    verifyWriteConsumer(500, 0);
  }

  @Test
  void writeAsyncPublisherConsumerFailFastTest() throws Exception {
    try {
      failFastExecutor.writeAsync(Flowable.just(FAILED), writeConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifyWrites(0);
      verifyWriteConsumer(0, 0);
    }
  }

  @Test
  void writeAsyncPublisherConsumerFailSafeTest() throws Exception {
    failSafeExecutor.writeAsync(sampleStatementsWithLastBad(), writeConsumer).get();
    verifyWrites(499);
    verifyWriteConsumer(499, 1);
  }

  // Tests for rx write methods

  @Test
  void writeReactiveStringTest() throws Exception {
    CsvUtils.queries().flatMap(failFastExecutor::writeReactive).blockingSubscribe();
    verifyWrites(500);
  }

  @Test
  void writeReactiveStringFailFastTest() throws Exception {
    try {
      sampleQueriesWithLastBad().flatMap(failFastExecutor::writeReactive).blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(499);
    }
  }

  @Test
  void writeReactiveStringFailSafeTest() throws Exception {
    Flowable.just(FAILED.getQueryString())
        .flatMap(failSafeExecutor::writeReactive)
        .blockingSubscribe();
  }

  @Test
  void writeReactiveStatementTest() throws Exception {
    CsvUtils.simpleStatements().flatMap(failFastExecutor::writeReactive).blockingSubscribe();
    verifyWrites(500);
  }

  @Test
  void writeReactiveStatementFailFastTest() throws Exception {
    try {
      sampleStatementsWithLastBad().flatMap(failFastExecutor::writeReactive).blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(499);
    }
  }

  @Test
  void writeReactiveStatementFailSafeTest() throws Exception {
    Flowable.just(FAILED).flatMap(failSafeExecutor::writeReactive).blockingSubscribe();
  }

  @Test
  void writeReactiveStreamTest() throws Exception {
    Stream<SimpleStatement> statements =
        stream(CsvUtils.simpleStatements().blockingIterable().spliterator(), false);
    Flowable.fromPublisher(failFastExecutor.writeReactive(statements)).blockingSubscribe();
    verifyWrites(500);
  }

  @Test
  void writeReactiveStreamFailFastTest() throws Exception {
    try {
      Stream<Statement> statements =
          stream(sampleStatementsWithLastBad().blockingIterable().spliterator(), false);
      Flowable.fromPublisher(failFastExecutor.writeReactive(statements)).blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(499);
    }
  }

  @Test
  void writeReactiveStreamFailSafeTest() throws Exception {
    Stream<Statement> statements =
        stream(sampleStatementsWithLastBad().blockingIterable().spliterator(), false);
    Flowable.fromPublisher(failSafeExecutor.writeReactive(statements)).blockingSubscribe();
    verifyWrites(499);
  }

  @Test
  void writeReactiveIterableTest() throws Exception {
    Iterable<SimpleStatement> statements = CsvUtils.simpleStatements().blockingIterable();
    Flowable.fromPublisher(failFastExecutor.writeReactive(statements)).blockingSubscribe();
    verifyWrites(500);
  }

  @Test
  void writeReactiveIterableFailFastTest() throws Exception {
    try {
      Iterable<Statement> statements = sampleStatementsWithLastBad().blockingIterable();
      Flowable.fromPublisher(failFastExecutor.writeReactive(statements)).blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(499);
    }
  }

  @Test
  void writeReactiveIterableFailSafeTest() throws Exception {
    Iterable<Statement> statements = sampleStatementsWithLastBad().blockingIterable();
    Flowable.fromPublisher(failSafeExecutor.writeReactive(statements)).blockingSubscribe();
    verifyWrites(499);
  }

  @Test
  void writeReactivePublisherTest() throws Exception {
    Flowable.fromPublisher(failFastExecutor.writeReactive(CsvUtils.simpleStatements()))
        .blockingSubscribe();
    verifyWrites(500);
  }

  @Test
  void writeReactivePublisherFailFastTest() throws Exception {
    try {
      Flowable.fromPublisher(failFastExecutor.writeReactive(sampleStatementsWithLastBad()))
          .blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(499);
    }
  }

  @Test
  void writeReactivePublisherFailSafeTest() throws Exception {
    Flowable.fromPublisher(failSafeExecutor.writeReactive(sampleStatementsWithLastBad()))
        .blockingSubscribe();
    verifyWrites(499);
  }

  // Tests for synchronous read methods

  @Test
  void readSyncStringConsumerTest() throws Exception {
    loadData();
    failFastExecutor.readSync(READ_SUCCESSFUL.getQueryString(), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  void readSyncStringConsumerFailFastTest() throws Exception {
    loadData();
    try {
      failFastExecutor.readSync(FAILED.getQueryString(), readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readSyncStringConsumerFailSafeTest() throws Exception {
    loadData();
    failSafeExecutor.readSync(FAILED.getQueryString(), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(0, 1);
    verifyReads(0, 1, readResults);
  }

  @Test
  void readSyncStatementConsumerTest() throws Exception {
    loadData();
    failFastExecutor.readSync(READ_SUCCESSFUL, readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  void readSyncStatementConsumerFailFastTest() throws Exception {
    loadData();
    try {
      failFastExecutor.readSync(FAILED, readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readSyncStatementConsumerFailSafeTest() throws Exception {
    loadData();
    failSafeExecutor.readSync(FAILED, readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(0, 1);
    verifyReads(0, 1, readResults);
  }

  @Test
  void readSyncStreamConsumerTest() {
    loadData();
    failFastExecutor.readSync(Stream.of(READ_SUCCESSFUL), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  void readSyncStreamConsumerFailFastTest() {
    loadData();
    try {
      failFastExecutor.readSync(Stream.of(FAILED), readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readSyncStreamConsumerFailSafeTest() {
    loadData();
    failSafeExecutor.readSync(Stream.of(READ_SUCCESSFUL, FAILED), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(500, 1);
    verifyReads(500, 1, readResults);
  }

  @Test
  void readSyncIterableConsumer() {
    loadData();
    failFastExecutor.readSync(Collections.singleton(READ_SUCCESSFUL), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  void readSyncIterableConsumerFailFastTest() {
    loadData();
    try {
      failFastExecutor.readSync(Collections.singleton(FAILED), readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readSyncIterableConsumerFailSafeTest() {
    loadData();
    failSafeExecutor.readSync(Arrays.asList(READ_SUCCESSFUL, FAILED), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(500, 1);
    verifyReads(500, 1, readResults);
  }

  @Test
  void readSyncPublisherConsumer() {
    loadData();
    failFastExecutor.readSync(Flowable.just(READ_SUCCESSFUL), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  void readSyncPublisherConsumerFailFastTest() {
    loadData();
    try {
      failFastExecutor.readSync(Flowable.just(FAILED), readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readSyncPublisherConsumerFailSafeTest() {
    loadData();
    failSafeExecutor.readSync(Flowable.fromArray(READ_SUCCESSFUL, FAILED), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(500, 1);
    verifyReads(500, 1, readResults);
  }

  // Tests for asynchronous read methods

  @Test
  void readAsyncStringConsumerTest() throws Exception {
    loadData();
    failSafeExecutor.readAsync(READ_SUCCESSFUL.getQueryString(), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  void readAsyncStringConsumerFailFastTest() throws Exception {
    loadData();
    try {
      failFastExecutor.readAsync(FAILED.getQueryString(), readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readAsyncStringConsumerFailSafeTest() throws Exception {
    loadData();
    failSafeExecutor.readAsync(FAILED.getQueryString(), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(0, 1);
    verifyReads(0, 1, readResults);
  }

  @Test
  void readAsyncStatementConsumerTest() throws Exception {
    loadData();
    failSafeExecutor.readAsync(READ_SUCCESSFUL, readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  void readAsyncStatementConsumerFailFastTest() throws Exception {
    loadData();
    try {
      failFastExecutor.readAsync(FAILED, readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readAsyncStatementConsumerFailSafeTest() throws Exception {
    loadData();
    failSafeExecutor.readAsync(FAILED, readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(0, 1);
    verifyReads(0, 1, readResults);
  }

  @Test
  void readAsyncStreamConsumerTest() throws Exception {
    loadData();
    failSafeExecutor.readAsync(Stream.of(READ_SUCCESSFUL), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  void readAsyncStreamConsumerFailFastTest() throws Exception {
    loadData();
    try {
      failFastExecutor.readAsync(Stream.of(FAILED), readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readAsyncStreamConsumerFailSafeTest() throws Exception {
    loadData();
    failSafeExecutor.readAsync(Stream.of(READ_SUCCESSFUL, FAILED), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(500, 1);
    verifyReads(500, 1, readResults);
  }

  @Test
  void readAsyncIterableConsumer() throws Exception {
    loadData();
    failSafeExecutor.readAsync(Collections.singleton(READ_SUCCESSFUL), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  void readAsyncIterableConsumerFailFastTest() throws Exception {
    loadData();
    try {
      failFastExecutor.readAsync(Collections.singleton(FAILED), readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readAsyncIterableConsumerFailSafeTest() throws Exception {
    loadData();
    failSafeExecutor.readAsync(Arrays.asList(READ_SUCCESSFUL, FAILED), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(500, 1);
    verifyReads(500, 1, readResults);
  }

  @Test
  void readAsyncPublisherConsumer() throws Exception {
    loadData();
    failSafeExecutor.readAsync(Flowable.just(READ_SUCCESSFUL), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  void readAsyncPublisherConsumerFailFastTest() throws Exception {
    loadData();
    try {
      failFastExecutor.readAsync(Flowable.fromArray(FAILED), readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readAsyncPublisherConsumerFailSafeTest() throws Exception {
    loadData();
    failSafeExecutor.readAsync(Flowable.fromArray(READ_SUCCESSFUL, FAILED), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(500, 1);
    verifyReads(500, 1, readResults);
  }

  // Tests for rx read methods

  @Test
  void readReactiveStringTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults =
        Flowable.just(READ_SUCCESSFUL.getQueryString())
            .flatMap(failFastExecutor::readReactive)
            .blockingIterable();
    verifyReads(500, 0, readResults);
  }

  @Test
  void readReactiveStringFailFastTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults = Collections.emptyList();
    try {
      Flowable.just(FAILED.getQueryString())
          .flatMap(failFastExecutor::readReactive)
          .blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readReactiveStringFailSafeTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults =
        Flowable.just(FAILED.getQueryString())
            .flatMap(failSafeExecutor::readReactive)
            .blockingIterable();
    verifyReads(0, 1, readResults);
  }

  @Test
  void readReactiveStatementTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults =
        Flowable.just(READ_SUCCESSFUL).flatMap(failFastExecutor::readReactive).blockingIterable();
    verifyReads(500, 0, readResults);
  }

  @Test
  void readReactiveStatementFailFastTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults = Collections.emptyList();
    try {
      Flowable.just(FAILED).flatMap(failFastExecutor::readReactive).blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readReactiveStatementFailSafeTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults =
        Flowable.just(FAILED).flatMap(failSafeExecutor::readReactive).blockingIterable();
    verifyReads(0, 1, readResults);
  }

  @Test
  void readReactiveStreamTest() throws Exception {
    loadData();
    Queue<ReadResult> readResults = new ConcurrentLinkedQueue<>();
    Flowable.fromPublisher(failFastExecutor.readReactive(Stream.of(READ_SUCCESSFUL)))
        .doOnNext(readResults::add)
        .blockingSubscribe();
    verifyReads(500, 0, readResults);
  }

  @Test
  void readReactiveStreamFailFastTest() throws Exception {
    loadData();
    Queue<ReadResult> readResults = new ConcurrentLinkedQueue<>();
    try {
      Flowable.fromPublisher(failFastExecutor.readReactive(Stream.of(FAILED)))
          .doOnNext(readResults::add)
          .blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readReactiveStreamFailSafeTest() throws Exception {
    loadData();
    Queue<ReadResult> readResults = new ConcurrentLinkedQueue<>();
    Flowable.fromPublisher(failSafeExecutor.readReactive(Stream.of(READ_SUCCESSFUL, FAILED)))
        .doOnNext(readResults::add)
        .blockingSubscribe();
    verifyReads(500, 1, readResults);
  }

  @Test
  void readReactiveIterableTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults =
        Flowable.fromPublisher(
                failFastExecutor.readReactive(Collections.singleton(READ_SUCCESSFUL)))
            .blockingIterable();
    verifyReads(500, 0, readResults);
  }

  @Test
  void readReactiveIterableFailFastTest() throws Exception {
    loadData();
    Queue<ReadResult> readResults = new ConcurrentLinkedQueue<>();
    try {
      Flowable.fromPublisher(failFastExecutor.readReactive(Collections.singleton(FAILED)))
          .doOnNext(readResults::add)
          .blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readReactiveIterableFailSafeTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults =
        Flowable.fromPublisher(
                failSafeExecutor.readReactive(Arrays.asList(READ_SUCCESSFUL, FAILED)))
            .blockingIterable();
    verifyReads(500, 1, readResults);
  }

  @Test
  void readReactivePublisherTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults =
        Flowable.fromPublisher(failFastExecutor.readReactive(Flowable.just(READ_SUCCESSFUL)))
            .blockingIterable();
    verifyReads(500, 0, readResults);
  }

  @Test
  void readReactivePublisherFailFastTest() throws Exception {
    loadData();
    Queue<ReadResult> readResults = new ConcurrentLinkedQueue<>();
    try {
      Flowable.fromPublisher(failFastExecutor.readReactive(Flowable.just(FAILED)))
          .doOnNext(readResults::add)
          .blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  void readReactivePublisherFailSafeTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults =
        Flowable.fromPublisher(
                failSafeExecutor.readReactive(Flowable.fromArray(READ_SUCCESSFUL, FAILED)))
            .toList()
            .blockingGet();
    verifyReads(500, 1, readResults);
  }

  private Flowable<Statement> sampleStatementsWithLastBad() {
    return CsvUtils.boundStatements(insertStatement)
        .cast(Statement.class)
        .skipLast(1)
        .concatWith(Flowable.<Statement>just(FAILED));
  }

  private Flowable<String> sampleQueriesWithLastBad() {
    return CsvUtils.queries().skipLast(1).concatWith(Flowable.just(FAILED.getQueryString()));
  }

  private void loadData() {
    CsvUtils.boundStatements(insertStatement)
        .flatMap(failFastExecutor::writeReactive)
        .blockingSubscribe();
  }

  private void verifyWrites(int expected) {
    long count =
        Flowable.fromPublisher(failFastExecutor.readReactive(READ_SUCCESSFUL))
            .doOnNext(
                r -> {
                  Assertions.assertThat(r.getRow().isPresent()).isTrue();
                  Row row = r.getRow().get();
                  Record record = CsvUtils.recordForRow(row);
                  Assertions.assertThat(record).isNotNull();
                  CsvUtils.assertRowEqualsRecord(row, record);
                })
            .count()
            .blockingGet();
    Assertions.assertThat(count).isEqualTo(expected);
  }

  private void verifyReads(
      int expectedSuccessful, int expectedFailed, Iterable<ReadResult> actual) {
    long actualSuccessful =
        Flowable.fromIterable(actual)
            .filter(Result::isSuccess)
            .doOnNext(
                r -> {
                  Assertions.assertThat(r.getRow().isPresent()).isTrue();
                  Row row = r.getRow().get();
                  Record record = CsvUtils.recordForRow(row);
                  Assertions.assertThat(record).isNotNull();
                  CsvUtils.assertRowEqualsRecord(row, record);
                })
            .count()
            .blockingGet();
    Assertions.assertThat(actualSuccessful).isEqualTo(expectedSuccessful);
    long actualFailed =
        Flowable.fromIterable(actual)
            .filter(r -> !r.isSuccess())
            .doOnNext(
                r -> {
                  Assertions.assertThat(r.getRow().isPresent()).isFalse();
                  Assertions.assertThat(r.getError().isPresent()).isTrue();
                  BulkExecutionException error = r.getError().get();
                  verifyException(error);
                })
            .count()
            .blockingGet();
    Assertions.assertThat(actualFailed).isEqualTo(expectedFailed);
  }

  private void verifySuccessfulWriteResult(WriteResult r) {
    Assertions.assertThat(r.isSuccess()).isTrue();
    Assertions.assertThat(((SimpleStatement) r.getStatement()).getQueryString())
        .isEqualTo(CsvUtils.firstQuery());
    Assertions.assertThat(r.getExecutionInfo().isPresent()).isTrue();
  }

  private void verifyFailedWriteResult(WriteResult r) {
    Assertions.assertThat(r.isSuccess()).isFalse();
    Assertions.assertThat(((SimpleStatement) r.getStatement()).getQueryString())
        .isEqualTo(FAILED.getQueryString());
    Assertions.assertThat(r.getExecutionInfo().isPresent()).isFalse();
  }

  private void verifyException(Throwable t) {
    Assertions.assertThat(t)
        .isInstanceOf(BulkExecutionException.class)
        .hasMessage(
            String.format(
                "Statement execution failed: %s "
                    + "(line 1:0 no viable alternative at input 'should' ([should]...))",
                FAILED.getQueryString()))
        .hasCauseExactlyInstanceOf(SyntaxError.class);
  }

  private void verifyWriteConsumer(int expectedSuccessful, int expectedFailed) {
    ArgumentCaptor<WriteResult> captor = ArgumentCaptor.forClass(WriteResult.class);
    Mockito.verify(writeConsumer, Mockito.times(expectedSuccessful + expectedFailed))
        .accept(captor.capture());
    List<WriteResult> values = captor.getAllValues();
    Assertions.assertThat(values.stream().filter(Result::isSuccess).count())
        .isEqualTo(expectedSuccessful);
    Assertions.assertThat(values.stream().filter(r -> !r.isSuccess()).count())
        .isEqualTo(expectedFailed);
    values
        .stream()
        .filter(Result::isSuccess)
        .forEach(
            r -> {
              Assertions.assertThat(r.getError().isPresent()).isFalse();
              Assertions.assertThat(r.getExecutionInfo().isPresent()).isTrue();
            });
    values
        .stream()
        .filter(r -> !r.isSuccess())
        .forEach(
            r -> {
              Assertions.assertThat(r.getError().isPresent()).isTrue();
              Assertions.assertThat(r.getExecutionInfo().isPresent()).isFalse();
            });
  }

  private List<ReadResult> verifyReadConsumer(int expectedSuccessful, int expectedFailed) {
    ArgumentCaptor<ReadResult> captor = ArgumentCaptor.forClass(ReadResult.class);
    Mockito.verify(readConsumer, Mockito.times(expectedSuccessful + expectedFailed))
        .accept(captor.capture());
    List<ReadResult> values = captor.getAllValues();
    Assertions.assertThat(values.stream().filter(Result::isSuccess).count())
        .isEqualTo(expectedSuccessful);
    Assertions.assertThat(values.stream().filter(r -> !r.isSuccess()).count())
        .isEqualTo(expectedFailed);
    values
        .stream()
        .filter(Result::isSuccess)
        .forEach(
            r -> {
              Assertions.assertThat(r.getError().isPresent()).isFalse();
              Assertions.assertThat(r.getRow().isPresent()).isTrue();
            });
    values
        .stream()
        .filter(r -> !r.isSuccess())
        .forEach(
            r -> {
              Assertions.assertThat(r.getError().isPresent()).isTrue();
              Assertions.assertThat(r.getRow().isPresent()).isFalse();
            });
    return values;
  }
}
