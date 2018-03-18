/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
import io.reactivex.Single;
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
  }

  DefaultRxJavaBulkExecutor(AbstractBulkExecutorBuilder builder) {
    super(builder);
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

  private class ReadResultPublisher implements Publisher<ReadResult> {

    private final Statement statement;

    private ReadResultPublisher(Statement statement) {
      this.statement = statement;
    }

    @Override
    public void subscribe(Subscriber<? super ReadResult> subscriber) {
      // As per rule 1.9, we need to throw an NPE if subscriber is null
      Objects.requireNonNull(subscriber, "Subscriber cannot be null");
      // As per rule 1.11, this publisher supports multiple subscribers in a unicast configuration,
      // i.e., each subscriber triggers an independent execution/subscription and gets its own copy
      // of the results.
      ReadResultSubscription subscription =
          new ReadResultSubscription(
              subscriber, statement, listener, requestPermits, rateLimiter, failFast);
      try {
        subscriber.onSubscribe(subscription);
        // must be called after onSubscribe
        subscription.start(() -> session.executeAsync(statement));
      } catch (Throwable t) {
        // As per rule 2.13: In the case that this rule is violated,
        // any associated Subscription to the Subscriber MUST be considered as
        // cancelled, and the caller MUST raise this error condition in a fashion
        // that is adequate for the runtime environment.
        subscription.doOnError(
            new IllegalStateException(
                subscriber
                    + " violated the Reactive Streams rule 2.13 by throwing an exception from onSubscribe.",
                t));
      }
      // As per 2.13, this method must return normally (i.e. not throw)
    }
  }

  private class WriteResultPublisher implements Publisher<WriteResult> {

    private final Statement statement;

    private WriteResultPublisher(Statement statement) {
      this.statement = statement;
    }

    @Override
    public void subscribe(Subscriber<? super WriteResult> subscriber) {
      // As per rule 1.9, we need to throw an NPE if subscriber is null
      Objects.requireNonNull(subscriber, "Subscriber cannot be null");
      // As per rule 1.11, this publisher supports multiple subscribers in a unicast configuration,
      // i.e., each subscriber triggers an independent execution/subscription and gets its own copy
      // of the results.
      WriteResultSubscription subscription =
          new WriteResultSubscription(
              subscriber, statement, listener, requestPermits, rateLimiter, failFast);
      try {
        subscriber.onSubscribe(subscription);
        // must be called after onSubscribe
        subscription.start(() -> session.executeAsync(statement));
      } catch (Throwable t) {
        // As per rule 2.13: In the case that this rule is violated,
        // any associated Subscription to the Subscriber MUST be considered as
        // cancelled, and the caller MUST raise this error condition in a fashion
        // that is adequate for the runtime environment.
        subscription.doOnError(
            new IllegalStateException(
                subscriber
                    + " violated the Reactive Streams rule 2.13 by throwing an exception from onSubscribe.",
                t));
      }
      // As per 2.13, this method must return normally (i.e. not throw)
    }
  }
}
