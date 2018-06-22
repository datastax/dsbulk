/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.rxjava;

import com.datastax.dsbulk.executor.api.AbstractBulkExecutor;
import com.datastax.dsbulk.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.dsbulk.executor.api.BulkExecutor;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.publisher.ReadResultPublisher;
import com.datastax.dsbulk.executor.api.internal.publisher.WriteResultPublisher;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import io.reactivex.Flowable;
import io.reactivex.Single;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;

/**
 * Animplementation of {@link BulkExecutor} using <a
 * href="https://github.com/ReactiveX/RxJava/wiki">RxJava</a>.
 */
public class DefaultRxJavaBulkExecutor extends AbstractBulkExecutor implements RxJavaBulkExecutor {

  /**
   * Creates a new builder of {@link DefaultRxJavaBulkExecutor} instances.
   *
   * @param session The {@link CqlSession} to use.
   * @return a new builder.
   */
  public static DefaultRxJavaBulkExecutorBuilder builder(CqlSession session) {
    return new DefaultRxJavaBulkExecutorBuilder(session);
  }

  /**
   * Creates a new instance using the given {@link CqlSession} and using defaults for all
   * parameters.
   *
   * <p>If you need to customize your executor, use the {@link #builder(CqlSession) builder} method
   * instead.
   *
   * @param session the {@link CqlSession} to use.
   */
  public DefaultRxJavaBulkExecutor(CqlSession session) {
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
    return Flowable.fromPublisher(
        new WriteResultPublisher(
            statement,
            session,
            failFast,
            listener,
            maxConcurrentRequests,
            maxConcurrentQueries,
            rateLimiter));
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
    return readReactive(SimpleStatement.newInstance(statement));
  }

  @Override
  public Flowable<ReadResult> readReactive(Statement statement) {
    Objects.requireNonNull(statement);
    return Flowable.fromPublisher(
        new ReadResultPublisher(
            statement,
            session,
            failFast,
            listener,
            maxConcurrentRequests,
            maxConcurrentQueries,
            rateLimiter));
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
}
