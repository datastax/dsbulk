/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.ccm;

import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.dsbulk.executor.api.BulkExecutor;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dsbulk.executor.api.result.Result;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.datastax.dsbulk.tests.ccm.CCMRule;
import com.datastax.dsbulk.tests.ccm.annotations.ClusterConfig;
import com.datastax.dsbulk.tests.utils.CsvUtils;
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
import javax.inject.Inject;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public abstract class AbstractBulkExecutorIT {

  private static final SimpleStatement readSuccessful =
      new SimpleStatement("SELECT * FROM ip_by_country");
  private static final SimpleStatement failed = new SimpleStatement("should fail");
  private static final String firstQuery = CsvUtils.firstQuery();

  @ClassRule public static CCMRule ccm = new CCMRule();

  @Mock private static Consumer<? super WriteResult> writeConsumer;

  @Mock private static Consumer<? super ReadResult> readConsumer;

  @Inject
  @ClusterConfig(queryOptions = "fetchSize:100" /* to force pagination */)
  static Session session;

  static PreparedStatement insertStatement;
  static BulkExecutor failFastExecutor;
  static BulkExecutor failSafeExecutor;

  @BeforeClass
  public static void createTableAndPreparedStatement() {
    CsvUtils.createIpByCountryTable(session);
    insertStatement = CsvUtils.prepareInsertStatement(session);
  }

  @BeforeClass
  public static void disableStackTraces() {
    RxJavaPlugins.setErrorHandler((t) -> {});
  }

  @Before
  public void resetMocks() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @After
  public void truncateTable() {
    CsvUtils.truncateIpByCountryTable(session);
    System.clearProperty("rx2.buffer-size");
  }

  // Tests for synchronous write methods

  @Test
  public void writeSyncStringTest() {
    WriteResult r = failFastExecutor.writeSync(firstQuery);
    verifySuccessfulWriteResult(r);
  }

  @Test
  public void writeSyncStringFailFastTest() {
    try {
      failFastExecutor.writeSync(failed.getQueryString());
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
    }
  }

  @Test
  public void writeSyncStringFailSafeTest() {
    WriteResult r = failSafeExecutor.writeSync(failed.getQueryString());
    verifyFailedWriteResult(r);
  }

  @Test
  public void writeSyncStatementTest() {
    WriteResult r = failFastExecutor.writeSync(firstQuery);
    verifySuccessfulWriteResult(r);
  }

  @Test
  public void writeSyncStatementFailFastTest() {
    try {
      failFastExecutor.writeSync(failed);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
    }
  }

  @Test
  public void writeSyncStatementFailSafeTest() {
    WriteResult r = failSafeExecutor.writeSync(failed);
    verifyFailedWriteResult(r);
  }

  @Test
  public void writeSyncStreamTest() {
    Stream<SimpleStatement> records =
        stream(CsvUtils.simpleStatements().blockingIterable().spliterator(), false);
    failFastExecutor.writeSync(records);
    verifyWrites(500);
  }

  @Test
  public void writeSyncStreamFailFastTest() {
    // The number of successful writes can vary if the failure occurs
    // out of order, due to concurrent processing of rows. Turn of
    // concurrency for this test to make results deterministic.
    System.setProperty("rx2.buffer-size", "1");
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
  public void writeSyncStreamFailSafeTest() {
    // The number of successful writes can vary if the failure occurs
    // out of order, due to concurrent processing of rows. Turn of
    // concurrency for this test to make results deterministic.
    System.setProperty("rx2.buffer-size", "1");

    Stream<Statement> records =
        stream(sampleStatementsWithLastBad().blockingIterable().spliterator(), false);
    failSafeExecutor.writeSync(records);
    verifyWrites(499);
  }

  @Test
  public void writeSyncStreamConsumerTest() {
    Stream<SimpleStatement> records =
        stream(CsvUtils.simpleStatements().blockingIterable().spliterator(), false);
    failFastExecutor.writeSync(records, writeConsumer);
    verifyWrites(500);
    verifyWriteConsumer(500, 0);
  }

  @Test
  public void writeSyncStreamConsumerFailFastTest() {
    try {
      failFastExecutor.writeSync(Stream.of(failed), writeConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(0);
      verifyWriteConsumer(0, 0);
    }
  }

  @Test
  public void writeSyncStreamConsumerFailSafeTest() {
    Stream<Statement> records =
        stream(sampleStatementsWithLastBad().blockingIterable().spliterator(), false);
    failSafeExecutor.writeSync(records, writeConsumer);
    verifyWrites(499);
    verifyWriteConsumer(499, 1);
  }

  @Test
  public void writeSyncIterableTest() {
    Iterable<SimpleStatement> records = CsvUtils.simpleStatements().blockingIterable();
    failFastExecutor.writeSync(records);
    verifyWrites(500);
  }

  @Test
  public void writeSyncIterableFailFastTest() {
    // The number of successful writes can vary if the failure occurs
    // out of order, due to concurrent processing of rows. Turn of
    // concurrency for this test to make results deterministic.
    System.setProperty("rx2.buffer-size", "1");
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
  public void writeSyncIterableFailSafeTest() {
    Iterable<Statement> records = sampleStatementsWithLastBad().blockingIterable();
    failSafeExecutor.writeSync(records);
    verifyWrites(499);
  }

  @Test
  public void writeSyncIterableConsumer() {
    Iterable<SimpleStatement> records = CsvUtils.simpleStatements().blockingIterable();
    failFastExecutor.writeSync(records, writeConsumer);
    verifyWrites(500);
    verifyWriteConsumer(500, 0);
  }

  @Test
  public void writeSyncIterableConsumerFailFastTest() {
    try {
      failFastExecutor.writeSync(Collections.singleton(failed), writeConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(0);
      verifyWriteConsumer(0, 0);
    }
  }

  @Test
  public void writeSyncIterableConsumerFailSafeTest() {
    Iterable<Statement> records = sampleStatementsWithLastBad().blockingIterable();
    failSafeExecutor.writeSync(records, writeConsumer);
    verifyWrites(499);
    verifyWriteConsumer(499, 1);
  }

  @Test
  public void writeSyncPublisherTest() {
    failFastExecutor.writeSync(CsvUtils.simpleStatements());
    verifyWrites(500);
  }

  @Test
  public void writeSyncPublisherFailFastTest() {
    // The number of successful writes can vary if the failure occurs
    // out of order, due to concurrent processing of rows. Turn of
    // concurrency for this test to make results deterministic.
    System.setProperty("rx2.buffer-size", "1");
    try {
      failFastExecutor.writeSync(sampleStatementsWithLastBad());
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(499);
    }
  }

  @Test
  public void writeSyncPublisherFailSafeTest() {
    failSafeExecutor.writeSync(sampleStatementsWithLastBad());
    verifyWrites(499);
  }

  @Test
  public void writeSyncPublisherConsumer() {
    failFastExecutor.writeSync(CsvUtils.simpleStatements(), writeConsumer);
    verifyWrites(500);
    verifyWriteConsumer(500, 0);
  }

  @Test
  public void writeSyncPublisherConsumerFailFastTest() {
    try {
      failFastExecutor.writeSync(Flowable.just(failed), writeConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(0);
      verifyWriteConsumer(0, 0);
    }
  }

  @Test
  public void writeSyncPublisherConsumerFailSafeTest() {
    failSafeExecutor.writeSync(sampleStatementsWithLastBad(), writeConsumer);
    verifyWrites(499);
    verifyWriteConsumer(499, 1);
  }

  // Tests for asynchronous write methods

  @Test
  public void writeAsyncStringTest() throws Exception {
    WriteResult r = failFastExecutor.writeAsync(CsvUtils.firstQuery()).get();
    verifySuccessfulWriteResult(r);
  }

  @Test
  public void writeAsyncStringFailFastTest() throws Exception {
    try {
      failFastExecutor.writeAsync(failed.getQueryString()).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
    }
  }

  @Test
  public void writeAsyncStringFailSafeTest() throws Exception {
    WriteResult r = failSafeExecutor.writeAsync(failed.getQueryString()).get();
    verifyFailedWriteResult(r);
  }

  @Test
  public void writeAsyncStatementTest() throws Exception {
    WriteResult r = failFastExecutor.writeAsync(CsvUtils.firstQuery()).get();
    verifySuccessfulWriteResult(r);
  }

  @Test
  public void writeAsyncStatementFailFastTest() throws Exception {
    try {
      failFastExecutor.writeAsync(failed).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
    }
  }

  @Test
  public void writeAsyncStatementFailSafeTest() throws Exception {
    WriteResult r = failSafeExecutor.writeAsync(failed).get();
    verifyFailedWriteResult(r);
  }

  @Test
  public void writeAsyncStreamTest() throws Exception {
    Stream<SimpleStatement> records =
        stream(CsvUtils.simpleStatements().blockingIterable().spliterator(), false);
    failFastExecutor.writeAsync(records).get();
    verifyWrites(500);
  }

  @Test
  public void writeAsyncStreamFailFastTest() throws Exception {
    // The number of successful writes can vary if the failure occurs
    // out of order, due to concurrent processing of rows. Turn of
    // concurrency for this test to make results deterministic.
    System.setProperty("rx2.buffer-size", "1");
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
  public void writeAsyncStreamFailSafeTest() throws Exception {
    Stream<Statement> records =
        stream(sampleStatementsWithLastBad().blockingIterable().spliterator(), false);
    failSafeExecutor.writeAsync(records).get();
    verifyWrites(499);
  }

  @Test
  public void writeAsyncStreamConsumerTest() throws Exception {
    Stream<SimpleStatement> records =
        stream(CsvUtils.simpleStatements().blockingIterable().spliterator(), false);
    failFastExecutor.writeAsync(records, writeConsumer).get();
    verifyWrites(500);
    verifyWriteConsumer(500, 0);
  }

  @Test
  public void writeAsyncStreamConsumerFailFastTest() throws Exception {
    try {
      failFastExecutor.writeAsync(Stream.of(failed), writeConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifyWrites(0);
      verifyWriteConsumer(0, 0);
    }
  }

  @Test
  public void writeAsyncStreamConsumerFailSafeTest() throws Exception {
    Stream<Statement> records =
        stream(sampleStatementsWithLastBad().blockingIterable().spliterator(), false);
    failSafeExecutor.writeAsync(records, writeConsumer).get();
    verifyWrites(499);
    verifyWriteConsumer(499, 1);
  }

  @Test
  public void writeAsyncIterableTest() throws Exception {
    Iterable<SimpleStatement> records = CsvUtils.simpleStatements().blockingIterable();
    failFastExecutor.writeAsync(records).get();
    verifyWrites(500);
  }

  @Test
  public void writeAsyncIterableFailFastTest() throws Exception {
    // The number of successful writes can vary if the failure occurs
    // out of order, due to concurrent processing of rows. Turn of
    // concurrency for this test to make results deterministic.
    System.setProperty("rx2.buffer-size", "1");
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
  public void writeAsyncIterableFailSafeTest() throws Exception {
    Iterable<Statement> records = sampleStatementsWithLastBad().blockingIterable();
    failSafeExecutor.writeAsync(records).get();
    verifyWrites(499);
  }

  @Test
  public void writeAsyncIterableConsumer() throws Exception {
    Iterable<SimpleStatement> records = CsvUtils.simpleStatements().blockingIterable();
    failFastExecutor.writeAsync(records, writeConsumer).get();
    verifyWrites(500);
    verifyWriteConsumer(500, 0);
  }

  @Test
  public void writeAsyncIterableConsumerFailFastTest() throws Exception {
    try {
      failFastExecutor.writeAsync(Collections.singleton(failed), writeConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifyWrites(0);
      verifyWriteConsumer(0, 0);
    }
  }

  @Test
  public void writeAsyncIterableConsumerFailSafeTest() throws Exception {
    Iterable<Statement> records = sampleStatementsWithLastBad().blockingIterable();
    failSafeExecutor.writeAsync(records, writeConsumer).get();
    verifyWrites(499);
    verifyWriteConsumer(499, 1);
  }

  @Test
  public void writeAsyncPublisherTest() throws Exception {
    failFastExecutor.writeAsync(CsvUtils.simpleStatements()).get();
    verifyWrites(500);
  }

  @Test
  public void writeAsyncPublisherFailFastTest() throws Exception {
    // The number of successful writes can vary if the failure occurs
    // out of order, due to concurrent processing of rows. Turn of
    // concurrency for this test to make results deterministic.
    System.setProperty("rx2.buffer-size", "1");
    try {
      failFastExecutor.writeAsync(sampleStatementsWithLastBad()).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifyWrites(499);
    }
  }

  @Test
  public void writeAsyncPublisherFailSafeTest() throws Exception {
    failSafeExecutor.writeAsync(sampleStatementsWithLastBad()).get();
    verifyWrites(499);
  }

  @Test
  public void writeAsyncPublisherConsumer() throws Exception {
    failFastExecutor.writeAsync(CsvUtils.simpleStatements(), writeConsumer).get();
    verifyWrites(500);
    verifyWriteConsumer(500, 0);
  }

  @Test
  public void writeAsyncPublisherConsumerFailFastTest() throws Exception {
    try {
      failFastExecutor.writeAsync(Flowable.just(failed), writeConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifyWrites(0);
      verifyWriteConsumer(0, 0);
    }
  }

  @Test
  public void writeAsyncPublisherConsumerFailSafeTest() throws Exception {
    failSafeExecutor.writeAsync(sampleStatementsWithLastBad(), writeConsumer).get();
    verifyWrites(499);
    verifyWriteConsumer(499, 1);
  }

  // Tests for rx write methods

  @Test
  public void writeReactiveStringTest() throws Exception {
    CsvUtils.queries().flatMap(failFastExecutor::writeReactive).blockingSubscribe();
    verifyWrites(500);
  }

  @Test
  public void writeReactiveStringFailFastTest() throws Exception {
    // The number of successful writes can vary if the failure occurs
    // out of order, due to concurrent processing of rows. Turn of
    // concurrency for this test to make results deterministic.
    System.setProperty("rx2.buffer-size", "1");
    try {
      sampleQueriesWithLastBad().flatMap(failFastExecutor::writeReactive).blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(499);
    }
  }

  @Test
  public void writeReactiveStringFailSafeTest() throws Exception {
    Flowable.just(failed.getQueryString())
        .flatMap(failSafeExecutor::writeReactive)
        .blockingSubscribe();
  }

  @Test
  public void writeReactiveStatementTest() throws Exception {
    CsvUtils.simpleStatements().flatMap(failFastExecutor::writeReactive).blockingSubscribe();
    verifyWrites(500);
  }

  @Test
  public void writeReactiveStatementFailFastTest() throws Exception {
    // The number of successful writes can vary if the failure occurs
    // out of order, due to concurrent processing of rows. Turn of
    // concurrency for this test to make results deterministic.
    System.setProperty("rx2.buffer-size", "1");
    try {
      sampleStatementsWithLastBad().flatMap(failFastExecutor::writeReactive).blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyWrites(499);
    }
  }

  @Test
  public void writeReactiveStatementFailSafeTest() throws Exception {
    Flowable.just(failed).flatMap(failSafeExecutor::writeReactive).blockingSubscribe();
  }

  @Test
  public void writeReactiveStreamTest() throws Exception {
    Stream<SimpleStatement> statements =
        stream(CsvUtils.simpleStatements().blockingIterable().spliterator(), false);
    Flowable.fromPublisher(failFastExecutor.writeReactive(statements)).blockingSubscribe();
    verifyWrites(500);
  }

  @Test
  public void writeReactiveStreamFailFastTest() throws Exception {
    // The number of successful writes can vary if the failure occurs
    // out of order, due to concurrent processing of rows. Turn of
    // concurrency for this test to make results deterministic.
    System.setProperty("rx2.buffer-size", "1");
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
  public void writeReactiveStreamFailSafeTest() throws Exception {
    Stream<Statement> statements =
        stream(sampleStatementsWithLastBad().blockingIterable().spliterator(), false);
    Flowable.fromPublisher(failSafeExecutor.writeReactive(statements)).blockingSubscribe();
    verifyWrites(499);
  }

  @Test
  public void writeReactiveIterableTest() throws Exception {
    Iterable<SimpleStatement> statements = CsvUtils.simpleStatements().blockingIterable();
    Flowable.fromPublisher(failFastExecutor.writeReactive(statements)).blockingSubscribe();
    verifyWrites(500);
  }

  @Test
  public void writeReactiveIterableFailFastTest() throws Exception {
    // The number of successful writes can vary if the failure occurs
    // out of order, due to concurrent processing of rows. Turn of
    // concurrency for this test to make results deterministic.
    System.setProperty("rx2.buffer-size", "1");
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
  public void writeReactiveIterableFailSafeTest() throws Exception {
    Iterable<Statement> statements = sampleStatementsWithLastBad().blockingIterable();
    Flowable.fromPublisher(failSafeExecutor.writeReactive(statements)).blockingSubscribe();
    verifyWrites(499);
  }

  @Test
  public void writeReactivePublisherTest() throws Exception {
    Flowable.fromPublisher(failFastExecutor.writeReactive(CsvUtils.simpleStatements()))
        .blockingSubscribe();
    verifyWrites(500);
  }

  @Test
  public void writeReactivePublisherFailFastTest() throws Exception {
    // The number of successful writes can vary if the failure occurs
    // out of order, due to concurrent processing of rows. Turn of
    // concurrency for this test to make results deterministic.
    System.setProperty("rx2.buffer-size", "1");
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
  public void writeReactivePublisherFailSafeTest() throws Exception {
    Flowable.fromPublisher(failSafeExecutor.writeReactive(sampleStatementsWithLastBad()))
        .blockingSubscribe();
    verifyWrites(499);
  }

  // Tests for synchronous read methods

  @Test
  public void readSyncStringConsumerTest() throws Exception {
    loadData();
    failFastExecutor.readSync(readSuccessful.getQueryString(), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  public void readSyncStringConsumerFailFastTest() throws Exception {
    loadData();
    try {
      failFastExecutor.readSync(failed.getQueryString(), readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  public void readSyncStringConsumerFailSafeTest() throws Exception {
    loadData();
    failSafeExecutor.readSync(failed.getQueryString(), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(0, 1);
    verifyReads(0, 1, readResults);
  }

  @Test
  public void readSyncStatementConsumerTest() throws Exception {
    loadData();
    failFastExecutor.readSync(readSuccessful, readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  public void readSyncStatementConsumerFailFastTest() throws Exception {
    loadData();
    try {
      failFastExecutor.readSync(failed, readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  public void readSyncStatementConsumerFailSafeTest() throws Exception {
    loadData();
    failSafeExecutor.readSync(failed, readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(0, 1);
    verifyReads(0, 1, readResults);
  }

  @Test
  public void readSyncStreamConsumerTest() {
    loadData();
    failFastExecutor.readSync(Stream.of(readSuccessful), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  public void readSyncStreamConsumerFailFastTest() {
    loadData();
    try {
      failFastExecutor.readSync(Stream.of(failed), readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  public void readSyncStreamConsumerFailSafeTest() {
    loadData();
    failSafeExecutor.readSync(Stream.of(readSuccessful, failed), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(500, 1);
    verifyReads(500, 1, readResults);
  }

  @Test
  public void readSyncIterableConsumer() {
    loadData();
    failFastExecutor.readSync(Collections.singleton(readSuccessful), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  public void readSyncIterableConsumerFailFastTest() {
    loadData();
    try {
      failFastExecutor.readSync(Collections.singleton(failed), readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  public void readSyncIterableConsumerFailSafeTest() {
    loadData();
    failSafeExecutor.readSync(Arrays.asList(readSuccessful, failed), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(500, 1);
    verifyReads(500, 1, readResults);
  }

  @Test
  public void readSyncPublisherConsumer() {
    loadData();
    failFastExecutor.readSync(Flowable.just(readSuccessful), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  public void readSyncPublisherConsumerFailFastTest() {
    loadData();
    try {
      failFastExecutor.readSync(Flowable.just(failed), readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  public void readSyncPublisherConsumerFailSafeTest() {
    loadData();
    failSafeExecutor.readSync(Flowable.fromArray(readSuccessful, failed), readConsumer);
    List<ReadResult> readResults = verifyReadConsumer(500, 1);
    verifyReads(500, 1, readResults);
  }

  // Tests for asynchronous read methods

  @Test
  public void readAsyncStringConsumerTest() throws Exception {
    loadData();
    failSafeExecutor.readAsync(readSuccessful.getQueryString(), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  public void readAsyncStringConsumerFailFastTest() throws Exception {
    loadData();
    try {
      failFastExecutor.readAsync(failed.getQueryString(), readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  public void readAsyncStringConsumerFailSafeTest() throws Exception {
    loadData();
    failSafeExecutor.readAsync(failed.getQueryString(), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(0, 1);
    verifyReads(0, 1, readResults);
  }

  @Test
  public void readAsyncStatementConsumerTest() throws Exception {
    loadData();
    failSafeExecutor.readAsync(readSuccessful, readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  public void readAsyncStatementConsumerFailFastTest() throws Exception {
    loadData();
    try {
      failFastExecutor.readAsync(failed, readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  public void readAsyncStatementConsumerFailSafeTest() throws Exception {
    loadData();
    failSafeExecutor.readAsync(failed, readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(0, 1);
    verifyReads(0, 1, readResults);
  }

  @Test
  public void readAsyncStreamConsumerTest() throws Exception {
    loadData();
    failSafeExecutor.readAsync(Stream.of(readSuccessful), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  public void readAsyncStreamConsumerFailFastTest() throws Exception {
    loadData();
    try {
      failFastExecutor.readAsync(Stream.of(failed), readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  public void readAsyncStreamConsumerFailSafeTest() throws Exception {
    loadData();
    failSafeExecutor.readAsync(Stream.of(readSuccessful, failed), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(500, 1);
    verifyReads(500, 1, readResults);
  }

  @Test
  public void readAsyncIterableConsumer() throws Exception {
    loadData();
    failSafeExecutor.readAsync(Collections.singleton(readSuccessful), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  public void readAsyncIterableConsumerFailFastTest() throws Exception {
    loadData();
    try {
      failFastExecutor.readAsync(Collections.singleton(failed), readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  public void readAsyncIterableConsumerFailSafeTest() throws Exception {
    loadData();
    failSafeExecutor.readAsync(Arrays.asList(readSuccessful, failed), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(500, 1);
    verifyReads(500, 1, readResults);
  }

  @Test
  public void readAsyncPublisherConsumer() throws Exception {
    loadData();
    failSafeExecutor.readAsync(Flowable.just(readSuccessful), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(500, 0);
    verifyReads(500, 0, readResults);
  }

  @Test
  public void readAsyncPublisherConsumerFailFastTest() throws Exception {
    loadData();
    try {
      failFastExecutor.readAsync(Flowable.fromArray(failed), readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      List<ReadResult> readResults = verifyReadConsumer(0, 0);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  public void readAsyncPublisherConsumerFailSafeTest() throws Exception {
    loadData();
    failSafeExecutor.readAsync(Flowable.fromArray(readSuccessful, failed), readConsumer).get();
    List<ReadResult> readResults = verifyReadConsumer(500, 1);
    verifyReads(500, 1, readResults);
  }

  // Tests for rx read methods

  @Test
  public void readReactiveStringTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults =
        Flowable.just(readSuccessful.getQueryString())
            .flatMap(failFastExecutor::readReactive)
            .blockingIterable();
    verifyReads(500, 0, readResults);
  }

  @Test
  public void readReactiveStringFailFastTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults = Collections.emptyList();
    try {
      Flowable.just(failed.getQueryString())
          .flatMap(failFastExecutor::readReactive)
          .blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  public void readReactiveStringFailSafeTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults =
        Flowable.just(failed.getQueryString())
            .flatMap(failSafeExecutor::readReactive)
            .blockingIterable();
    verifyReads(0, 1, readResults);
  }

  @Test
  public void readReactiveStatementTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults =
        Flowable.just(readSuccessful).flatMap(failFastExecutor::readReactive).blockingIterable();
    verifyReads(500, 0, readResults);
  }

  @Test
  public void readReactiveStatementFailFastTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults = Collections.emptyList();
    try {
      Flowable.just(failed).flatMap(failFastExecutor::readReactive).blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  public void readReactiveStatementFailSafeTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults =
        Flowable.just(failed).flatMap(failSafeExecutor::readReactive).blockingIterable();
    verifyReads(0, 1, readResults);
  }

  @Test
  public void readReactiveStreamTest() throws Exception {
    loadData();
    Queue<ReadResult> readResults = new ConcurrentLinkedQueue<>();
    Flowable.fromPublisher(failFastExecutor.readReactive(Stream.of(readSuccessful)))
        .doOnNext(readResults::add)
        .blockingSubscribe();
    verifyReads(500, 0, readResults);
  }

  @Test
  public void readReactiveStreamFailFastTest() throws Exception {
    loadData();
    Queue<ReadResult> readResults = new ConcurrentLinkedQueue<>();
    try {
      Flowable.fromPublisher(failFastExecutor.readReactive(Stream.of(failed)))
          .doOnNext(readResults::add)
          .blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  public void readReactiveStreamFailSafeTest() throws Exception {
    loadData();
    Queue<ReadResult> readResults = new ConcurrentLinkedQueue<>();
    Flowable.fromPublisher(failSafeExecutor.readReactive(Stream.of(readSuccessful, failed)))
        .doOnNext(readResults::add)
        .blockingSubscribe();
    verifyReads(500, 1, readResults);
  }

  @Test
  public void readReactiveIterableTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults =
        Flowable.fromPublisher(failFastExecutor.readReactive(Collections.singleton(readSuccessful)))
            .blockingIterable();
    verifyReads(500, 0, readResults);
  }

  @Test
  public void readReactiveIterableFailFastTest() throws Exception {
    loadData();
    Queue<ReadResult> readResults = new ConcurrentLinkedQueue<>();
    try {
      Flowable.fromPublisher(failFastExecutor.readReactive(Collections.singleton(failed)))
          .doOnNext(readResults::add)
          .blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  public void readReactiveIterableFailSafeTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults =
        Flowable.fromPublisher(failSafeExecutor.readReactive(Arrays.asList(readSuccessful, failed)))
            .blockingIterable();
    verifyReads(500, 1, readResults);
  }

  @Test
  public void readReactivePublisherTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults =
        Flowable.fromPublisher(failFastExecutor.readReactive(Flowable.just(readSuccessful)))
            .blockingIterable();
    verifyReads(500, 0, readResults);
  }

  @Test
  public void readReactivePublisherFailFastTest() throws Exception {
    loadData();
    Queue<ReadResult> readResults = new ConcurrentLinkedQueue<>();
    try {
      Flowable.fromPublisher(failFastExecutor.readReactive(Flowable.just(failed)))
          .doOnNext(readResults::add)
          .blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifyReads(0, 0, readResults);
    }
  }

  @Test
  public void readReactivePublisherFailSafeTest() throws Exception {
    loadData();
    Iterable<ReadResult> readResults =
        Flowable.fromPublisher(
                failSafeExecutor.readReactive(Flowable.fromArray(readSuccessful, failed)))
            .toList()
            .blockingGet();
    verifyReads(500, 1, readResults);
  }

  private Flowable<Statement> sampleStatementsWithLastBad() {
    return CsvUtils.boundStatements(insertStatement)
        .cast(Statement.class)
        .skipLast(1)
        .concatWith(Flowable.<Statement>just(failed));
  }

  private Flowable<String> sampleQueriesWithLastBad() {
    return CsvUtils.queries().skipLast(1).concatWith(Flowable.just(failed.getQueryString()));
  }

  private void loadData() {
    CsvUtils.boundStatements(insertStatement)
        .flatMap(failFastExecutor::writeReactive)
        .blockingSubscribe();
  }

  private void verifyWrites(int expected) {
    long count =
        Flowable.fromPublisher(failFastExecutor.readReactive(readSuccessful))
            .doOnNext(
                r -> {
                  Assertions.assertThat(r.getRow().isPresent());
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
        .isEqualTo(failed.getQueryString());
    Assertions.assertThat(r.getExecutionInfo().isPresent()).isFalse();
  }

  private void verifyException(Throwable t) {
    Assertions.assertThat(t)
        .isInstanceOf(BulkExecutionException.class)
        .hasMessage(
            String.format(
                "Statement execution failed: %s "
                    + "(line 1:0 no viable alternative at input 'should' ([should]...))",
                failed.getQueryString()))
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
