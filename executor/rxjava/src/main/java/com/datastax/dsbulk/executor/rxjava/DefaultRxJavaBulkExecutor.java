/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.rxjava;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.AbstractBulkExecutor;
import com.datastax.dsbulk.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.dsbulk.executor.api.BulkExecutor;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.subscription.ReadResultSubscription;
import com.datastax.dsbulk.executor.api.internal.subscription.WriteResultSubscription;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * Animplementation of {@link BulkExecutor} using <a
 * href="https://github.com/ReactiveX/RxJava/wiki">RxJava</a>.
 */
public class DefaultRxJavaBulkExecutor extends AbstractBulkExecutor implements RxJavaBulkExecutor {

  /**
   * Creates a new builder of {@link DefaultRxJavaBulkExecutor} instances.
   *
   * @param session The {@link Session} to use.
   * @return a new builder.
   */
  public static DefaultRxJavaBulkExecutorBuilder builder(Session session) {
    return new DefaultRxJavaBulkExecutorBuilder(session);
  }

  private final Scheduler scheduler;

  /**
   * Creates a new instance using the given {@link Session} and using defaults for all parameters.
   *
   * <p>If you need to customize your executor, use the {@link #builder(Session) builder} method
   * instead.
   *
   * @param session the {@link Session} to use.
   */
  public DefaultRxJavaBulkExecutor(Session session) {
    super(session);
    this.scheduler = Schedulers.from(executor);
  }

  DefaultRxJavaBulkExecutor(AbstractBulkExecutorBuilder builder) {
    super(builder);
    this.scheduler = Schedulers.from(this.executor);
  }

  @Override
  public void writeSync(
      Stream<? extends Statement> statements, Consumer<? super WriteResult> consumer)
      throws BulkExecutionException {
    writeSync(Flowable.fromIterable(statements::iterator), consumer);
  }

  @Override
  public void writeSync(
      Iterable<? extends Statement> statements, Consumer<? super WriteResult> consumer)
      throws BulkExecutionException {
    writeSync(Flowable.fromIterable(statements), consumer);
  }

  @Override
  public CompletableFuture<WriteResult> writeAsync(Statement statement) {
    CompletableFuture<WriteResult> future = new CompletableFuture<>();
    Single.fromPublisher(writeReactive(statement))
        .doOnSuccess(future::complete)
        .doOnError(future::completeExceptionally)
        .subscribeOn(scheduler)
        .subscribe();
    return future;
  }

  @Override
  public CompletableFuture<Void> writeAsync(
      Stream<? extends Statement> statements, Consumer<? super WriteResult> consumer)
      throws BulkExecutionException {
    return writeAsync(Flowable.fromIterable(statements::iterator), consumer);
  }

  @Override
  public CompletableFuture<Void> writeAsync(
      Iterable<? extends Statement> statements, Consumer<? super WriteResult> consumer)
      throws BulkExecutionException {
    return writeAsync(Flowable.fromIterable(statements), consumer);
  }

  @Override
  public CompletableFuture<Void> writeAsync(
      Publisher<? extends Statement> statements, Consumer<? super WriteResult> consumer) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    Flowable.fromPublisher(statements)
        .flatMap(this::writeReactive)
        .doOnNext(consumer::accept)
        .doOnComplete(() -> future.complete(null))
        .doOnError(future::completeExceptionally)
        .subscribeOn(scheduler)
        .subscribe();
    return future;
  }

  @Override
  public Flowable<WriteResult> writeReactive(Statement statement) {
    Objects.requireNonNull(statement);
    return Flowable.fromPublisher(new WriteResultPublisher(statement));
  }

  @Override
  public Flowable<WriteResult> writeReactive(Stream<? extends Statement> statements)
      throws BulkExecutionException {
    return writeReactive(Flowable.fromIterable(statements::iterator));
  }

  @Override
  public Flowable<WriteResult> writeReactive(Iterable<? extends Statement> statements)
      throws BulkExecutionException {
    return writeReactive(Flowable.fromIterable(statements));
  }

  @Override
  public Flowable<WriteResult> writeReactive(Publisher<? extends Statement> statements)
      throws BulkExecutionException {
    return Flowable.fromPublisher(statements).flatMap(this::writeReactive);
  }

  @Override
  public void readSync(
      Stream<? extends Statement> statements, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException {
    readSync(Flowable.fromIterable(statements::iterator), consumer);
  }

  @Override
  public CompletableFuture<Void> readAsync(
      Statement statement, Consumer<? super ReadResult> consumer) throws BulkExecutionException {
    return readAsync(Flowable.just(statement), consumer);
  }

  @Override
  public CompletableFuture<Void> readAsync(
      Stream<? extends Statement> statements, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException {
    return readAsync(Flowable.fromIterable(statements::iterator), consumer);
  }

  @Override
  public void readSync(
      Iterable<? extends Statement> statements, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException {
    readSync(Flowable.fromIterable(statements), consumer);
  }

  @Override
  public CompletableFuture<Void> readAsync(
      Iterable<? extends Statement> statements, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException {
    return readAsync(Flowable.fromIterable(statements), consumer);
  }

  @Override
  public CompletableFuture<Void> readAsync(
      Publisher<? extends Statement> statements, Consumer<? super ReadResult> consumer) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    Flowable.fromPublisher(statements)
        .flatMap(this::readReactive)
        .doOnNext(consumer::accept)
        .doOnComplete(() -> future.complete(null))
        .doOnError(future::completeExceptionally)
        .subscribeOn(scheduler)
        .subscribe();
    return future;
  }

  @Override
  public Flowable<ReadResult> readReactive(String statement) throws BulkExecutionException {
    return readReactive(new SimpleStatement(statement));
  }

  @Override
  public Flowable<ReadResult> readReactive(Statement statement) {
    Objects.requireNonNull(statement);
    return Flowable.fromPublisher(new ReadResultPublisher(statement));
  }

  @Override
  public Flowable<ReadResult> readReactive(Stream<? extends Statement> statements)
      throws BulkExecutionException {
    return readReactive(Flowable.fromIterable(statements::iterator));
  }

  @Override
  public Flowable<ReadResult> readReactive(Iterable<? extends Statement> statements)
      throws BulkExecutionException {
    return readReactive(Flowable.fromIterable(statements));
  }

  @Override
  public Flowable<ReadResult> readReactive(Publisher<? extends Statement> statements)
      throws BulkExecutionException {
    return Flowable.fromPublisher(statements).flatMap(this::readReactive);
  }

  @Override
  public void close() throws InterruptedException {
    super.close();
    scheduler.shutdown();
  }

  private class ReadResultPublisher implements Publisher<ReadResult> {

    private final Statement statement;

    private ReadResultPublisher(Statement statement) {
      this.statement = statement;
    }

    @Override
    public void subscribe(Subscriber<? super ReadResult> subscriber) {
      ReadResultSubscription subscription =
          new ReadResultSubscription(
              subscriber,
              queueFactory.newQueue(statement),
              statement,
              session,
              executor,
              listener,
              rateLimiter,
              requestPermits,
              failFast);
      subscriber.onSubscribe(subscription);
      subscription.start();
    }
  }

  private class WriteResultPublisher implements Publisher<WriteResult> {

    private final Statement statement;

    private WriteResultPublisher(Statement statement) {
      this.statement = statement;
    }

    @Override
    public void subscribe(Subscriber<? super WriteResult> subscriber) {
      WriteResultSubscription subscription =
          new WriteResultSubscription(
              subscriber,
              statement,
              session,
              executor,
              listener,
              rateLimiter,
              requestPermits,
              failFast);
      subscriber.onSubscribe(subscription);
      subscription.start();
    }
  }
}
