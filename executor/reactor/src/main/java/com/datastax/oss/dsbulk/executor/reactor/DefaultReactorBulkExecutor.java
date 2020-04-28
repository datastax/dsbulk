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
package com.datastax.oss.dsbulk.executor.reactor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.dsbulk.executor.api.AbstractBulkExecutor;
import com.datastax.oss.dsbulk.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.oss.dsbulk.executor.api.BulkExecutor;
import com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.oss.dsbulk.executor.api.publisher.ReadResultPublisher;
import com.datastax.oss.dsbulk.executor.api.publisher.WriteResultPublisher;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.dsbulk.executor.api.result.WriteResult;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * An implementation of {@link BulkExecutor} using <a href="https://projectreactor.io">Reactor</a>.
 */
public class DefaultReactorBulkExecutor extends AbstractBulkExecutor
    implements ReactorBulkExecutor {

  /**
   * Creates a new builder of {@link DefaultReactorBulkExecutor} instances.
   *
   * @param session The {@link CqlSession} to use.
   * @return a new builder.
   */
  public static DefaultReactorBulkExecutorBuilder builder(CqlSession session) {
    return new DefaultReactorBulkExecutorBuilder(session);
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
  public DefaultReactorBulkExecutor(CqlSession session) {
    super(session);
  }

  DefaultReactorBulkExecutor(AbstractBulkExecutorBuilder builder) {
    super(builder);
  }

  @Override
  public void writeSync(
      Stream<? extends Statement> statements, Consumer<? super WriteResult> consumer)
      throws BulkExecutionException {
    writeSync(Flux.fromStream(statements), consumer);
  }

  @Override
  public void writeSync(
      Iterable<? extends Statement> statements, Consumer<? super WriteResult> consumer)
      throws BulkExecutionException {
    writeSync(Flux.fromIterable(statements), consumer);
  }

  @Override
  public CompletableFuture<WriteResult> writeAsync(Statement<?> statement) {
    CompletableFuture<WriteResult> future = new CompletableFuture<>();
    Mono.from(writeReactive(statement))
        .doOnSuccess(future::complete)
        .doOnError(future::completeExceptionally)
        .subscribe();
    return future;
  }

  @Override
  public CompletableFuture<Void> writeAsync(
      Stream<? extends Statement> statements, Consumer<? super WriteResult> consumer)
      throws BulkExecutionException {
    return writeAsync(Flux.fromStream(statements), consumer);
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
        .doOnNext(consumer)
        .doOnComplete(() -> future.complete(null))
        .doOnError(future::completeExceptionally)
        .subscribe();
    return future;
  }

  @Override
  public Mono<WriteResult> writeReactive(Statement<?> statement) {
    Objects.requireNonNull(statement);
    return Mono.from(
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
  public Flux<WriteResult> writeReactive(Stream<? extends Statement> statements)
      throws BulkExecutionException {
    return writeReactive(Flux.fromStream(statements));
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
    readSync(Flux.fromStream(statements), consumer);
  }

  @Override
  public CompletableFuture<Void> readAsync(
      Statement<?> statement, Consumer<? super ReadResult> consumer) throws BulkExecutionException {
    return readAsync(Flux.just(statement), consumer);
  }

  @Override
  public CompletableFuture<Void> readAsync(
      Stream<? extends Statement> statements, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException {
    return readAsync(Flux.fromStream(statements), consumer);
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
        .doOnNext(consumer)
        .doOnComplete(() -> future.complete(null))
        .doOnError(future::completeExceptionally)
        .subscribe();
    return future;
  }

  @Override
  public Flux<ReadResult> readReactive(String statement) throws BulkExecutionException {
    return readReactive(SimpleStatement.newInstance(statement));
  }

  @Override
  public Flux<ReadResult> readReactive(Statement<?> statement) {
    Objects.requireNonNull(statement);
    return Flux.from(
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
  public Flux<ReadResult> readReactive(Stream<? extends Statement> statements)
      throws BulkExecutionException {
    return readReactive(Flux.fromStream(statements));
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
}
