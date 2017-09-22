/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api;

import static reactor.core.publisher.FluxSink.OverflowStrategy.BUFFER;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.emitter.BackpressureController;
import com.datastax.dsbulk.executor.api.internal.emitter.ReadResultEmitter;
import com.datastax.dsbulk.executor.api.internal.emitter.WriteResultEmitter;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * An implementation of {@link BulkExecutor} using <a href="https://projectreactor.io">Reactor</a>.
 */
public class DefaultReactorBulkExecutor extends AbstractBulkExecutor
    implements ReactorBulkExecutor {

  /**
   * Creates a new builder of {@link DefaultReactorBulkExecutor} instances.
   *
   * @param session The {@link Session} to use.
   * @return a new builder.
   */
  public static DefaultReactorBulkExecutorBuilder builder(Session session) {
    return new DefaultReactorBulkExecutorBuilder(session);
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
  DefaultReactorBulkExecutor(Session session) {
    super(session);
    this.scheduler = Schedulers.fromExecutor(executor);
  }

  DefaultReactorBulkExecutor(
      Session session,
      boolean failFast,
      int maxInFlightRequests,
      int maxRequestsPerSecond,
      ExecutionListener listener,
      Executor executor) {
    super(session, failFast, maxInFlightRequests, maxRequestsPerSecond, listener, executor);
    this.scheduler = Schedulers.fromExecutor(this.executor);
  }

  @Override
  public void writeSync(
      Stream<? extends Statement> statements, Consumer<? super WriteResult> consumer)
      throws BulkExecutionException {
    writeSync(Flux.fromIterable(statements::iterator), consumer);
  }

  @Override
  public void writeSync(
      Iterable<? extends Statement> statements, Consumer<? super WriteResult> consumer)
      throws BulkExecutionException {
    writeSync(Flux.fromIterable(statements), consumer);
  }

  @Override
  public CompletableFuture<WriteResult> writeAsync(Statement statement) {
    CompletableFuture<WriteResult> future = new CompletableFuture<>();
    Mono.from(writeReactive(statement))
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
    return writeAsync(Flux.fromIterable(statements::iterator), consumer);
  }

  @Override
  public CompletableFuture<Void> writeAsync(
      Iterable<? extends Statement> statements, Consumer<? super WriteResult> consumer)
      throws BulkExecutionException {
    return writeAsync(Flux.fromIterable(statements), consumer);
  }

  @Override
  public CompletableFuture<Void> writeAsync(
      Publisher<? extends Statement> statements, Consumer<? super WriteResult> consumer) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    Flux.from(statements)
        .flatMap(this::writeReactive)
        .doOnNext(consumer::accept)
        .doOnComplete(() -> future.complete(null))
        .doOnError(future::completeExceptionally)
        .subscribeOn(scheduler)
        .subscribe();
    return future;
  }

  @Override
  public Mono<WriteResult> writeReactive(Statement statement) {
    Objects.requireNonNull(statement);
    return Mono.create(
        e -> {
          ReactorWriteResultEmitter emitter = new ReactorWriteResultEmitter(statement, e);
          emitter.start();
        });
  }

  @Override
  public Flux<WriteResult> writeReactive(Stream<? extends Statement> statements)
      throws BulkExecutionException {
    return writeReactive(Flux.fromIterable(statements::iterator));
  }

  @Override
  public Flux<WriteResult> writeReactive(Iterable<? extends Statement> statements)
      throws BulkExecutionException {
    return writeReactive(Flux.fromIterable(statements));
  }

  @Override
  public Flux<WriteResult> writeReactive(Publisher<? extends Statement> statements)
      throws BulkExecutionException {
    return Flux.from(statements).flatMap(this::writeReactive);
  }

  @Override
  public void readSync(
      Stream<? extends Statement> statements, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException {
    readSync(Flux.fromIterable(statements::iterator), consumer);
  }

  @Override
  public CompletableFuture<Void> readAsync(
      Statement statement, Consumer<? super ReadResult> consumer) throws BulkExecutionException {
    return readAsync(Flux.just(statement), consumer);
  }

  @Override
  public CompletableFuture<Void> readAsync(
      Stream<? extends Statement> statements, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException {
    return readAsync(Flux.fromIterable(statements::iterator), consumer);
  }

  @Override
  public void readSync(
      Iterable<? extends Statement> statements, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException {
    readSync(Flux.fromIterable(statements), consumer);
  }

  @Override
  public CompletableFuture<Void> readAsync(
      Iterable<? extends Statement> statements, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException {
    return readAsync(Flux.fromIterable(statements), consumer);
  }

  @Override
  public CompletableFuture<Void> readAsync(
      Publisher<? extends Statement> statements, Consumer<? super ReadResult> consumer) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    Flux.from(statements)
        .flatMap(this::readReactive)
        .doOnNext(consumer::accept)
        .doOnComplete(() -> future.complete(null))
        .doOnError(future::completeExceptionally)
        .subscribeOn(scheduler)
        .subscribe();
    return future;
  }

  @Override
  public Flux<ReadResult> readReactive(String statement) throws BulkExecutionException {
    return readReactive(new SimpleStatement(statement));
  }

  @Override
  public Flux<ReadResult> readReactive(Statement statement) {
    Objects.requireNonNull(statement);
    return Flux.create(
        e -> {
          ReactorReadResultEmitter emitter = new ReactorReadResultEmitter(statement, e);
          emitter.start();
        },
        BUFFER);
  }

  @Override
  public Flux<ReadResult> readReactive(Stream<? extends Statement> statements)
      throws BulkExecutionException {
    return readReactive(Flux.fromIterable(statements::iterator));
  }

  @Override
  public Flux<ReadResult> readReactive(Iterable<? extends Statement> statements)
      throws BulkExecutionException {
    return readReactive(Flux.fromIterable(statements));
  }

  @Override
  public Flux<ReadResult> readReactive(Publisher<? extends Statement> statements)
      throws BulkExecutionException {
    return Flux.from(statements).flatMap(this::readReactive);
  }

  @Override
  public void close() throws InterruptedException {
    super.close();
    scheduler.dispose();
  }

  private class ReactorReadResultEmitter extends ReadResultEmitter implements LongConsumer {

    private final FluxSink<ReadResult> sink;
    private final BackpressureController demand = new BackpressureController();

    private ReactorReadResultEmitter(Statement statement, FluxSink<ReadResult> sink) {
      super(
          statement,
          DefaultReactorBulkExecutor.this.session,
          executor,
          listener,
          rateLimiter,
          requestPermits,
          failFast);
      this.sink = sink;
      sink.onRequest(this);
    }

    @Override
    public void accept(long requested) {
      demand.signalRequested(requested);
    }

    @Override
    protected void notifyOnNext(ReadResult result) {
      demand.awaitRequested(1);
      sink.next(result);
    }

    @Override
    protected void notifyOnComplete() {
      sink.complete();
    }

    @Override
    protected void notifyOnError(BulkExecutionException error) {
      sink.error(error);
    }

    @Override
    protected boolean isCancelled() {
      return sink.isCancelled();
    }
  }

  private class ReactorWriteResultEmitter extends WriteResultEmitter {

    private final MonoSink<WriteResult> sink;
    private volatile WriteResult result;

    private ReactorWriteResultEmitter(Statement statement, MonoSink<WriteResult> sink) {
      super(
          statement,
          DefaultReactorBulkExecutor.this.session,
          executor,
          listener,
          rateLimiter,
          requestPermits,
          failFast);
      this.sink = sink;
    }

    @Override
    protected void notifyOnNext(WriteResult result) {
      this.result = result;
    }

    @Override
    protected void notifyOnComplete() {
      sink.success(result);
    }

    @Override
    protected void notifyOnError(BulkExecutionException error) {
      sink.error(error);
    }

    @Override
    protected boolean isCancelled() {
      return false;
    }
  }
}
