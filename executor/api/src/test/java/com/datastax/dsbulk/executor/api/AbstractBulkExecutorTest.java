/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.listener.ExecutionContext;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dsbulk.executor.api.result.Result;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import io.reactivex.Flowable;
import io.reactivex.plugins.RxJavaPlugins;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("Duplicates")
public abstract class AbstractBulkExecutorTest {

  private final SimpleStatement successful1 = new SimpleStatement("should succeed");
  private final SimpleStatement successful2 = new SimpleStatement("should succeed 2");
  private final SimpleStatement failed = new SimpleStatement("should fail");

  protected final Session session = mock(Session.class);

  private Consumer<? super WriteResult> writeConsumer;
  private Consumer<? super ReadResult> readConsumer;

  protected ExecutionListener listener;

  @BeforeClass
  public static void disableStackTraces() throws Exception {
    RxJavaPlugins.setErrorHandler((t) -> {});
  }

  @SuppressWarnings("unchecked")
  @Before
  public void setUpConsumers() throws ExecutionException, InterruptedException {
    writeConsumer = mock(Consumer.class);
    readConsumer = mock(Consumer.class);
  }

  @SuppressWarnings("unchecked")
  @Before
  public void setUpListener() throws ExecutionException, InterruptedException {
    listener = mock(ExecutionListener.class);
  }

  protected abstract BulkExecutor newBulkExecutor(boolean failSafe);

  // Tests for synchronous write methods

  @Test
  public void writeSyncStringTest() {
    BulkExecutor executor = newBulkExecutor(false);
    WriteResult r = executor.writeSync("should succeed");
    verifySuccessfulWriteResult(r);
    verifySession(1, 0);
  }

  @Test
  public void writeSyncStringFailFastTest() {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.writeSync("should fail");
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(0, 1);
    }
  }

  @Test
  public void writeSyncStringFailSafeTest() {
    BulkExecutor executor = newBulkExecutor(true);
    WriteResult r = executor.writeSync("should fail");
    verifyFailedWriteResult(r);
    verifySession(0, 1);
    verifyListener(0, 1);
  }

  @Test
  public void writeSyncStatementTest() {
    BulkExecutor executor = newBulkExecutor(false);
    WriteResult r = executor.writeSync(successful1);
    verifySuccessfulWriteResult(r);
    verifySession(1, 0);
  }

  @Test
  public void writeSyncStatementFailFastTest() {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.writeSync(failed);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(0, 1);
    }
  }

  @Test
  public void writeSyncStatementFailSafeTest() {
    BulkExecutor executor = newBulkExecutor(true);
    WriteResult r = executor.writeSync(failed);
    verifyFailedWriteResult(r);
    verifySession(0, 1);
    verifyListener(0, 1);
  }

  @Test
  public void writeSyncStreamTest() {
    BulkExecutor executor = newBulkExecutor(false);
    executor.writeSync(Stream.of(successful1, successful2));
    verifySession(2, 0);
  }

  @Test
  public void writeSyncStreamFailFastTest() {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.writeSync(Stream.of(successful1, failed));
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(1, 1);
    }
  }

  @Test
  public void writeSyncStreamFailSafeTest() {
    BulkExecutor executor = newBulkExecutor(true);
    executor.writeSync(Stream.of(successful1, failed));
    verifySession(1, 1);
    verifyListener(1, 1);
  }

  @Test
  public void writeSyncStreamConsumerTest() {
    BulkExecutor executor = newBulkExecutor(false);
    executor.writeSync(Stream.of(successful1, successful2), writeConsumer);
    verifySession(2, 0);
    verifyWriteConsumer(2, 0);
  }

  @Test
  public void writeSyncStreamConsumerFailFastTest() {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.writeSync(Stream.of(successful1, failed), writeConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(1, 1);
      verifyWriteConsumer(1, 0);
    }
  }

  @Test
  public void writeSyncStreamConsumerFailSafeTest() {
    BulkExecutor executor = newBulkExecutor(true);
    executor.writeSync(Stream.of(successful1, failed), writeConsumer);
    verifySession(1, 1);
    verifyListener(1, 1);
    verifyWriteConsumer(1, 1);
  }

  @Test
  public void writeSyncIterableTest() {
    BulkExecutor executor = newBulkExecutor(false);
    executor.writeSync(Arrays.asList(successful1, successful2));
    verifySession(2, 0);
  }

  @Test
  public void writeSyncIterableFailFastTest() {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.writeSync(Arrays.asList(successful1, failed));
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(1, 1);
    }
  }

  @Test
  public void writeSyncIterableFailSafeTest() {
    BulkExecutor executor = newBulkExecutor(true);
    executor.writeSync(Arrays.asList(successful1, failed));
    verifySession(1, 1);
    verifyListener(1, 1);
  }

  @Test
  public void writeSyncIterableConsumer() {
    BulkExecutor executor = newBulkExecutor(false);
    executor.writeSync(Arrays.asList(successful1, successful2), writeConsumer);
    verifySession(2, 0);
    verifyWriteConsumer(2, 0);
  }

  @Test
  public void writeSyncIterableConsumerFailFastTest() {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.writeSync(Arrays.asList(successful1, failed), writeConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(1, 1);
      verifyWriteConsumer(1, 0);
    }
  }

  @Test
  public void writeSyncIterableConsumerFailSafeTest() {
    BulkExecutor executor = newBulkExecutor(true);
    executor.writeSync(Arrays.asList(successful1, failed), writeConsumer);
    verifySession(1, 1);
    verifyListener(1, 1);
    verifyWriteConsumer(1, 1);
  }

  @Test
  public void writeSyncPublisherTest() {
    BulkExecutor executor = newBulkExecutor(false);
    executor.writeSync(Flowable.fromArray(successful1, successful2));
    verifySession(2, 0);
  }

  @Test
  public void writeSyncPublisherFailFastTest() {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.writeSync(Flowable.fromArray(successful1, failed));
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(1, 1);
    }
  }

  @Test
  public void writeSyncPublisherFailSafeTest() {
    BulkExecutor executor = newBulkExecutor(true);
    executor.writeSync(Flowable.fromArray(successful1, failed));
    verifySession(1, 1);
    verifyListener(1, 1);
  }

  @Test
  public void writeSyncPublisherConsumer() {
    BulkExecutor executor = newBulkExecutor(false);
    executor.writeSync(Flowable.fromArray(successful1, successful2), writeConsumer);
    verifySession(2, 0);
    verifyWriteConsumer(2, 0);
  }

  @Test
  public void writeSyncPublisherConsumerFailFastTest() {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.writeSync(Flowable.fromArray(successful1, failed), writeConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(1, 1);
      verifyWriteConsumer(1, 0);
    }
  }

  @Test
  public void writeSyncPublisherConsumerFailSafeTest() {
    BulkExecutor executor = newBulkExecutor(true);
    executor.writeSync(Flowable.fromArray(successful1, failed), writeConsumer);
    verifySession(1, 1);
    verifyListener(1, 1);
    verifyWriteConsumer(1, 1);
  }

  // Tests for asynchronous write methods

  @Test
  public void writeAsyncStringTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    WriteResult r = executor.writeAsync("should succeed").get();
    verifySuccessfulWriteResult(r);
    verifySession(1, 0);
  }

  @Test
  public void writeAsyncStringFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.writeAsync("should fail").get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifySession(0, 1);
    }
  }

  @Test
  public void writeAsyncStringFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    WriteResult r = executor.writeAsync("should fail").get();
    verifyFailedWriteResult(r);
    verifySession(0, 1);
    verifyListener(0, 1);
  }

  @Test
  public void writeAsyncStatementTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    WriteResult r = executor.writeAsync(successful1).get();
    verifySuccessfulWriteResult(r);
    verifySession(1, 0);
  }

  @Test
  public void writeAsyncStatementFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.writeAsync(failed).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifySession(0, 1);
    }
  }

  @Test
  public void writeAsyncStatementFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    WriteResult r = executor.writeAsync(failed).get();
    verifyFailedWriteResult(r);
    verifySession(0, 1);
    verifyListener(0, 1);
  }

  @Test
  public void writeAsyncStreamTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    executor.writeAsync(Stream.of(successful1, successful2)).get();
    verifySession(2, 0);
  }

  @Test
  public void writeAsyncStreamFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.writeAsync(Stream.of(successful1, failed)).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifySession(1, 1);
    }
  }

  @Test
  public void writeAsyncStreamFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    executor.writeAsync(Stream.of(successful1, failed)).get();
    verifySession(1, 1);
    verifyListener(1, 1);
  }

  @Test
  public void writeAsyncStreamConsumerTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    executor.writeAsync(Stream.of(successful1, successful2), writeConsumer).get();
    verifySession(2, 0);
    verifyWriteConsumer(2, 0);
  }

  @Test
  public void writeAsyncStreamConsumerFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.writeAsync(Stream.of(successful1, failed), writeConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifySession(1, 1);
      verifyWriteConsumer(1, 0);
    }
  }

  @Test
  public void writeAsyncStreamConsumerFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    executor.writeAsync(Stream.of(successful1, failed), writeConsumer).get();
    verifySession(1, 1);
    verifyListener(0, 1);
    verifyWriteConsumer(1, 1);
  }

  @Test
  public void writeAsyncIterableTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    executor.writeAsync(Arrays.asList(successful1, successful2)).get();
    verifySession(2, 0);
  }

  @Test
  public void writeAsyncIterableFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.writeAsync(Arrays.asList(successful1, failed)).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifySession(1, 1);
    }
  }

  @Test
  public void writeAsyncIterableFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    executor.writeAsync(Arrays.asList(successful1, failed)).get();
    verifySession(1, 1);
    verifyListener(1, 1);
  }

  @Test
  public void writeAsyncIterableConsumer() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    executor.writeAsync(Arrays.asList(successful1, successful2), writeConsumer).get();
    verifySession(2, 0);
    verifyWriteConsumer(2, 0);
  }

  @Test
  public void writeAsyncIterableConsumerFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.writeAsync(Arrays.asList(successful1, failed), writeConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifySession(1, 1);
      verifyWriteConsumer(1, 0);
    }
  }

  @Test
  public void writeAsyncIterableConsumerFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    executor.writeAsync(Arrays.asList(successful1, failed), writeConsumer).get();
    verifySession(1, 1);
    verifyListener(1, 1);
    verifyWriteConsumer(1, 1);
  }

  @Test
  public void writeAsyncPublisherTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    executor.writeAsync(Flowable.fromArray(successful1, successful2)).get();
    verifySession(2, 0);
  }

  @Test
  public void writeAsyncPublisherFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.writeAsync(Flowable.fromArray(successful1, failed)).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifySession(1, 1);
    }
  }

  @Test
  public void writeAsyncPublisherFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    executor.writeAsync(Flowable.fromArray(successful1, failed)).get();
    verifySession(1, 1);
    verifyListener(1, 1);
  }

  @Test
  public void writeAsyncPublisherConsumer() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    executor.writeAsync(Flowable.fromArray(successful1, successful2), writeConsumer).get();
    verifySession(2, 0);
    verifyWriteConsumer(2, 0);
  }

  @Test
  public void writeAsyncPublisherConsumerFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.writeAsync(Flowable.fromArray(successful1, failed), writeConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifySession(1, 1);
      verifyWriteConsumer(1, 0);
    }
  }

  @Test
  public void writeAsyncPublisherConsumerFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    executor.writeAsync(Flowable.fromArray(successful1, failed), writeConsumer).get();
    verifySession(1, 1);
    verifyListener(1, 1);
    verifyWriteConsumer(1, 1);
  }

  // Tests for rx write methods

  @Test
  public void writeReactiveStringTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    Flowable.just("should succeed").flatMap(executor::writeReactive).blockingSubscribe();
    verifySession(1, 0);
  }

  @Test
  public void writeReactiveStringFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      Flowable.just("should fail").flatMap(executor::writeReactive).blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(0, 1);
    }
  }

  @Test
  public void writeReactiveStringFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    Flowable.just("should fail").flatMap(executor::writeReactive).blockingSubscribe();
    verifySession(0, 1);
    verifyListener(0, 1);
  }

  @Test
  public void writeReactiveStatementTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    Flowable.just(successful1).flatMap(executor::writeReactive).blockingSubscribe();
    verifySession(1, 0);
  }

  @Test
  public void writeReactiveStatementFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      Flowable.just(failed).flatMap(executor::writeReactive).blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(0, 1);
    }
  }

  @Test
  public void writeReactiveStatementFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    Flowable.just(failed).flatMap(executor::writeReactive).blockingSubscribe();
    verifySession(0, 1);
    verifyListener(0, 1);
  }

  @Test
  public void writeReactiveStreamTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    Flowable.fromPublisher(executor.writeReactive(Stream.of(successful1, successful2)))
        .blockingSubscribe();
    verifySession(2, 0);
  }

  @Test
  public void writeReactiveStreamFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      Flowable.fromPublisher(executor.writeReactive(Stream.of(successful1, failed)))
          .blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(1, 1);
    }
  }

  @Test
  public void writeReactiveStreamFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    Flowable.fromPublisher(executor.writeReactive(Stream.of(successful1, failed)))
        .blockingSubscribe();
    verifySession(1, 1);
    verifyListener(1, 1);
  }

  @Test
  public void writeReactiveIterableTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    Flowable.fromPublisher(executor.writeReactive(Arrays.asList(successful1, successful2)))
        .blockingSubscribe();
    verifySession(2, 0);
  }

  @Test
  public void writeReactiveIterableFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      Flowable.fromPublisher(executor.writeReactive(Arrays.asList(successful1, failed)))
          .blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(1, 1);
    }
  }

  @Test
  public void writeReactiveIterableFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    Flowable.fromPublisher(executor.writeReactive(Arrays.asList(successful1, failed)))
        .blockingSubscribe();
    verifySession(1, 1);
    verifyListener(1, 1);
  }

  @Test
  public void writeReactivePublisherTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    Flowable.fromPublisher(executor.writeReactive(Flowable.fromArray(successful1, successful2)))
        .blockingSubscribe();
    verifySession(2, 0);
  }

  @Test
  public void writeReactivePublisherFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      Flowable.fromPublisher(executor.writeReactive(Flowable.fromArray(successful1, failed)))
          .blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(1, 1);
    }
  }

  @Test
  public void writeReactivePublisherFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    Flowable.fromPublisher(executor.writeReactive(Flowable.fromArray(successful1, failed)))
        .blockingSubscribe();
    verifySession(1, 1);
    verifyListener(1, 1);
  }

  // Tests for synchronous read methods

  @Test
  public void readSyncStringConsumerTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    executor.readSync("should succeed", readConsumer);
    verifySession(1, 0);
    verifyReadConsumer(4, 0);
  }

  @Test
  public void readSyncStringConsumerFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.readSync("should fail", readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(0, 1);
      verifyReadConsumer(0, 0);
    }
  }

  @Test
  public void readSyncStringConsumerFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    executor.readSync("should fail", readConsumer);
    verifySession(0, 1);
    verifyListener(0, 1);
    verifyReadConsumer(0, 1);
  }

  @Test
  public void readSyncStatementConsumerTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    executor.readSync(successful1, readConsumer);
    verifySession(1, 0);
    verifyReadConsumer(4, 0);
  }

  @Test
  public void readSyncStatementConsumerFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.readSync(failed, readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(0, 1);
      verifyReadConsumer(0, 0);
    }
  }

  @Test
  public void readSyncStatementConsumerFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    executor.readSync(failed, readConsumer);
    verifySession(0, 1);
    verifyListener(0, 1);
    verifyReadConsumer(0, 1);
  }

  @Test
  public void readSyncStreamConsumerTest() {
    BulkExecutor executor = newBulkExecutor(false);
    executor.readSync(Stream.of(successful1, successful2), readConsumer);
    verifySession(2, 0);
    verifyReadConsumer(5, 0);
  }

  @Test
  public void readSyncStreamConsumerFailFastTest() {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.readSync(Stream.of(successful1, failed), readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(1, 1);
      verifyReadConsumer(4, 0);
    }
  }

  @Test
  public void readSyncStreamConsumerFailSafeTest() {
    BulkExecutor executor = newBulkExecutor(true);
    executor.readSync(Stream.of(successful1, failed), readConsumer);
    verifySession(1, 1);
    verifyListener(1, 1);
    verifyReadConsumer(4, 1);
  }

  @Test
  public void readSyncIterableConsumer() {
    BulkExecutor executor = newBulkExecutor(false);
    executor.readSync(Arrays.asList(successful1, successful2), readConsumer);
    verifySession(2, 0);
    verifyReadConsumer(5, 0);
  }

  @Test
  public void readSyncIterableConsumerFailFastTest() {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.readSync(Arrays.asList(successful1, failed), readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(1, 1);
      verifyReadConsumer(4, 0);
    }
  }

  @Test
  public void readSyncIterableConsumerFailSafeTest() {
    BulkExecutor executor = newBulkExecutor(true);
    executor.readSync(Arrays.asList(successful1, failed), readConsumer);
    verifySession(1, 1);
    verifyListener(1, 1);
    verifyReadConsumer(4, 1);
  }

  @Test
  public void readSyncPublisherConsumer() {
    BulkExecutor executor = newBulkExecutor(false);
    executor.readSync(Flowable.fromArray(successful1, successful2), readConsumer);
    verifySession(2, 0);
    verifyReadConsumer(5, 0);
  }

  @Test
  public void readSyncPublisherConsumerFailFastTest() {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.readSync(Flowable.fromArray(successful1, failed), readConsumer);
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(1, 1);
      verifyReadConsumer(4, 0);
    }
  }

  @Test
  public void readSyncPublisherConsumerFailSafeTest() {
    BulkExecutor executor = newBulkExecutor(true);
    executor.readSync(Flowable.fromArray(successful1, failed), readConsumer);
    verifySession(1, 1);
    verifyListener(1, 1);
    verifyReadConsumer(4, 1);
  }

  // Tests for asynchronous read methods

  @Test
  public void readAsyncStringConsumerTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    executor.readAsync("should succeed", readConsumer).get();
    verifySession(1, 0);
    verifyReadConsumer(4, 0);
  }

  @Test
  public void readAsyncStringConsumerFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.readAsync("should fail", readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifySession(0, 1);
      verifyReadConsumer(0, 0);
    }
  }

  @Test
  public void readAsyncStringConsumerFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    executor.readAsync("should fail", readConsumer).get();
    verifySession(0, 1);
    verifyListener(0, 1);
    verifyReadConsumer(0, 1);
  }

  @Test
  public void readAsyncStatementConsumerTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    executor.readAsync(successful1, readConsumer).get();
    verifySession(1, 0);
    verifyReadConsumer(4, 0);
  }

  @Test
  public void readAsyncStatementConsumerFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.readAsync(failed, readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifySession(0, 1);
      verifyReadConsumer(0, 0);
    }
  }

  @Test
  public void readAsyncStatementConsumerFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    executor.readAsync(failed, readConsumer).get();
    verifySession(0, 1);
    verifyListener(0, 1);
    verifyReadConsumer(0, 1);
  }

  @Test
  public void readAsyncStreamConsumerTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    executor.readAsync(Stream.of(successful1, successful2), readConsumer).get();
    verifySession(2, 0);
    verifyReadConsumer(5, 0);
  }

  @Test
  public void readAsyncStreamConsumerFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.readAsync(Stream.of(successful1, failed), readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifySession(1, 1);
      verifyReadConsumer(4, 0);
    }
  }

  @Test
  public void readAsyncStreamConsumerFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    executor.readAsync(Stream.of(successful1, failed), readConsumer).get();
    verifySession(1, 1);
    verifyListener(1, 1);
    verifyReadConsumer(4, 1);
  }

  @Test
  public void readAsyncIterableConsumer() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    executor.readAsync(Arrays.asList(successful1, successful2), readConsumer).get();
    verifySession(2, 0);
    verifyReadConsumer(5, 0);
  }

  @Test
  public void readAsyncIterableConsumerFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.readAsync(Arrays.asList(successful1, failed), readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifySession(1, 1);
      verifyReadConsumer(4, 0);
    }
  }

  @Test
  public void readAsyncIterableConsumerFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    executor.readAsync(Arrays.asList(successful1, failed), readConsumer).get();
    verifySession(1, 1);
    verifyListener(1, 1);
    verifyReadConsumer(4, 1);
  }

  @Test
  public void readAsyncPublisherConsumer() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    executor.readAsync(Flowable.fromArray(successful1, successful2), readConsumer).get();
    verifySession(2, 0);
    verifyReadConsumer(5, 0);
  }

  @Test
  public void readAsyncPublisherConsumerFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      executor.readAsync(Flowable.fromArray(successful1, failed), readConsumer).get();
      fail("Should have thrown an exception");
    } catch (ExecutionException e) {
      verifyException(e.getCause());
      verifySession(1, 1);
      verifyReadConsumer(4, 0);
    }
  }

  @Test
  public void readAsyncPublisherConsumerFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    executor.readAsync(Flowable.fromArray(successful1, failed), readConsumer).get();
    verifySession(1, 1);
    verifyListener(1, 1);
    verifyReadConsumer(4, 1);
  }

  // Tests for rx read methods

  @Test
  public void readReactiveStringTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    Flowable.just("should succeed").flatMap(executor::readReactive).blockingSubscribe();
    verifySession(1, 0);
  }

  @Test
  public void readReactiveStringFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      Flowable.just("should fail").flatMap(executor::readReactive).blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(0, 1);
    }
  }

  @Test
  public void readReactiveStringFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    Flowable.just("should fail").flatMap(executor::readReactive).blockingSubscribe();
    verifySession(0, 1);
    verifyListener(0, 1);
  }

  @Test
  public void readReactiveStatementTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    Flowable.just(successful1).flatMap(executor::readReactive).blockingSubscribe();
    verifySession(1, 0);
  }

  @Test
  public void readReactiveStatementFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      Flowable.just(failed).flatMap(executor::readReactive).blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(0, 1);
    }
  }

  @Test
  public void readReactiveStatementFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    Flowable.just(successful1).flatMap(executor::readReactive).blockingSubscribe();
    verifySession(1, 0);
    verifyListener(1, 0);
  }

  @Test
  public void readReactiveStreamTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    Flowable.fromPublisher(executor.readReactive(Stream.of(successful1, successful2)))
        .blockingSubscribe();
    verifySession(2, 0);
  }

  @Test
  public void readReactiveStreamFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      Flowable.fromPublisher(executor.readReactive(Stream.of(successful1, failed)))
          .blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(1, 1);
    }
  }

  @Test
  public void readReactiveStreamFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    Flowable.fromPublisher(executor.readReactive(Stream.of(successful1, failed)))
        .blockingSubscribe();
    verifySession(1, 1);
    verifyListener(1, 1);
  }

  @Test
  public void readReactiveIterableTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    Flowable.fromPublisher(executor.readReactive(Arrays.asList(successful1, successful2)))
        .blockingSubscribe();
    verifySession(2, 0);
  }

  @Test
  public void readReactiveIterableFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      Flowable.fromPublisher(executor.readReactive(Arrays.asList(successful1, failed)))
          .blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(1, 1);
    }
  }

  @Test
  public void readReactiveIterableFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    Flowable.fromPublisher(executor.readReactive(Arrays.asList(successful1, failed)))
        .blockingSubscribe();
    verifySession(1, 1);
    verifyListener(1, 1);
  }

  @Test
  public void readReactivePublisherTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(false);
    Flowable.fromPublisher(executor.readReactive(Flowable.fromArray(successful1, successful2)))
        .blockingSubscribe();
    verifySession(2, 0);
  }

  @Test
  public void readReactivePublisherFailFastTest() throws Exception {
    try {
      BulkExecutor executor = newBulkExecutor(false);
      Flowable.fromPublisher(executor.readReactive(Flowable.fromArray(successful1, failed)))
          .blockingSubscribe();
      fail("Should have thrown an exception");
    } catch (BulkExecutionException e) {
      verifyException(e);
      verifySession(1, 1);
    }
  }

  @Test
  public void readReactivePublisherFailSafeTest() throws Exception {
    BulkExecutor executor = newBulkExecutor(true);
    Flowable.fromPublisher(executor.readReactive(Flowable.fromArray(successful1, failed)))
        .blockingSubscribe();
    verifySession(1, 1);
    verifyListener(1, 1);
  }

  private void verifySession(int expectedSuccessful, int expectedFailed) {
    if (expectedSuccessful > 0) {
      verify(session).executeAsync(argThat(new StatementMatcher(successful1)));
      if (expectedSuccessful > 1) {
        verify(session).executeAsync(argThat(new StatementMatcher(successful2)));
      }
    }
    verify(session, times(expectedFailed)).executeAsync(argThat(new StatementMatcher(failed)));
  }

  private void verifyListener(int expectedSuccessful, int expectedFailed) {
    if (expectedSuccessful > 0) {
      verify(listener)
          .onExecutionStarted(
              argThat(new StatementMatcher(successful1)), any(ExecutionContext.class));
      verify(listener)
          .onExecutionSuccessful(
              argThat(new StatementMatcher(successful1)), any(ExecutionContext.class));
      if (expectedSuccessful > 1) {
        verify(listener)
            .onExecutionStarted(
                argThat(new StatementMatcher(successful2)), any(ExecutionContext.class));
        verify(listener)
            .onExecutionSuccessful(
                argThat(new StatementMatcher(successful2)), any(ExecutionContext.class));
      }
    }
    verify(listener, times(expectedFailed))
        .onExecutionStarted(argThat(new StatementMatcher(failed)), any(ExecutionContext.class));
    verify(listener, times(expectedFailed))
        .onExecutionFailed(
            argThat(new BulkExecutionExceptionMatcher(failed, SyntaxError.class)),
            any(ExecutionContext.class));
    verify(listener, never())
        .onExecutionFailed(
            argThat(new BulkExecutionExceptionMatcher(successful1)), any(ExecutionContext.class));
    verify(listener, never())
        .onExecutionFailed(
            argThat(new BulkExecutionExceptionMatcher(successful2)), any(ExecutionContext.class));
    verify(listener, never())
        .onExecutionSuccessful(argThat(new StatementMatcher(failed)), any(ExecutionContext.class));
  }

  private void verifySuccessfulWriteResult(WriteResult r) {
    assertThat(r.isSuccess()).isTrue();
    assertThat(((SimpleStatement) r.getStatement()).getQueryString())
        .isEqualTo(successful1.getQueryString());
    assertThat(r.getExecutionInfo().isPresent()).isTrue();
  }

  private void verifyFailedWriteResult(WriteResult r) {
    assertThat(r.isSuccess()).isFalse();
    assertThat(((SimpleStatement) r.getStatement()).getQueryString())
        .isEqualTo(failed.getQueryString());
    assertThat(r.getExecutionInfo().isPresent()).isFalse();
  }

  private void verifyException(Throwable t) {
    assertThat(t)
        .isInstanceOf(BulkExecutionException.class)
        .hasMessage(
            String.format(
                "Statement execution failed: %s (%s)",
                failed, "line 1:0 no viable alternative at input 'should' ([should]...)"))
        .hasCauseExactlyInstanceOf(SyntaxError.class);
  }

  private void verifyWriteConsumer(int expectedSuccessful, int expectedFailed) {
    ArgumentCaptor<WriteResult> captor = ArgumentCaptor.forClass(WriteResult.class);
    verify(writeConsumer, times(expectedSuccessful + expectedFailed)).accept(captor.capture());
    List<WriteResult> values = captor.getAllValues();
    assertThat(values.stream().filter(Result::isSuccess).count()).isEqualTo(expectedSuccessful);
    assertThat(values.stream().filter(r -> !r.isSuccess()).count()).isEqualTo(expectedFailed);
    values
        .stream()
        .filter(Result::isSuccess)
        .forEach(
            r -> {
              assertThat(r.getError().isPresent()).isFalse();
              assertThat(r.getExecutionInfo().isPresent()).isTrue();
            });
    values
        .stream()
        .filter(r -> !r.isSuccess())
        .forEach(
            r -> {
              assertThat(r.getError().isPresent()).isTrue();
              assertThat(r.getExecutionInfo().isPresent()).isFalse();
            });
    verifyStatements(values);
  }

  private void verifyReadConsumer(int expectedSuccessful, int expectedFailed) {
    ArgumentCaptor<ReadResult> captor = ArgumentCaptor.forClass(ReadResult.class);
    verify(readConsumer, times(expectedSuccessful + expectedFailed)).accept(captor.capture());
    List<ReadResult> values = captor.getAllValues();
    assertThat(values.stream().filter(Result::isSuccess).count()).isEqualTo(expectedSuccessful);
    assertThat(values.stream().filter(r -> !r.isSuccess()).count()).isEqualTo(expectedFailed);
    values
        .stream()
        .filter(Result::isSuccess)
        .forEach(
            r -> {
              assertThat(r.getError().isPresent()).isFalse();
              assertThat(r.getRow().isPresent()).isTrue();
            });
    values
        .stream()
        .filter(r -> !r.isSuccess())
        .forEach(
            r -> {
              assertThat(r.getError().isPresent()).isTrue();
              assertThat(r.getRow().isPresent()).isFalse();
            });
    verifyStatements(values);
  }

  private void verifyStatements(List<? extends Result> values) {
    values
        .stream()
        .filter(Result::isSuccess)
        .map(Result::getStatement)
        .map(this::toQueryString)
        .forEach(s -> assertThat(s).startsWith("should succeed"));
    values
        .stream()
        .filter(r -> !r.isSuccess())
        .map(Result::getStatement)
        .map(this::toQueryString)
        .forEach(s -> assertThat(s).startsWith("should fail"));
  }

  private String toQueryString(Statement statement) {
    return statement instanceof SimpleStatement
        ? ((SimpleStatement) statement).getQueryString()
        : ((BoundStatement) statement).preparedStatement().getQueryString();
  }

  private static class StatementMatcher extends BaseMatcher<Statement> {

    private final SimpleStatement stmt;

    private StatementMatcher(SimpleStatement stmt) {
      this.stmt = stmt;
    }

    @Override
    public boolean matches(Object item) {
      return item instanceof SimpleStatement
              && ((SimpleStatement) item).getQueryString().equals(stmt.getQueryString())
          || item instanceof BoundStatement
              && ((BoundStatement) item)
                  .preparedStatement()
                  .getQueryString()
                  .equals(stmt.getQueryString());
    }

    @Override
    public void describeTo(Description description) {}
  }

  private static class BulkExecutionExceptionMatcher extends BaseMatcher<BulkExecutionException> {

    private final SimpleStatement stmt;
    private final Class<? extends Exception> clazz;

    public BulkExecutionExceptionMatcher(SimpleStatement stmt) {
      this(stmt, null);
    }

    private BulkExecutionExceptionMatcher(SimpleStatement stmt, Class<? extends Exception> clazz) {
      this.stmt = stmt;
      this.clazz = clazz;
    }

    @Override
    public boolean matches(Object item) {
      if (item instanceof BulkExecutionException) {
        BulkExecutionException bee = (BulkExecutionException) item;
        Statement stmt = bee.getStatement();
        if (stmt instanceof SimpleStatement
                && ((SimpleStatement) stmt).getQueryString().equals(this.stmt.getQueryString())
            || stmt instanceof BoundStatement
                && ((BoundStatement) stmt)
                    .preparedStatement()
                    .getQueryString()
                    .equals(this.stmt.getQueryString())) {
          if (clazz == null) return true;
          Throwable cause = bee.getCause();
          if (cause.getClass().equals(clazz)) return true;
        }
      }
      return false;
    }

    @Override
    public void describeTo(Description description) {}
  }
}
