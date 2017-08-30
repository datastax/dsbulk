/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.writer;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;

/**
 * An asynchronous execution unit for bulk write operations.
 *
 * <p>Methods of this interface all return {@link CompletableFuture completable futures} that will
 * complete when the whole write operation completes.
 */
public interface AsyncBulkWriter extends AutoCloseable {

  /**
   * Executes the given write statement asynchronously.
   *
   * @param statement The statement to execute.
   * @return A {@link CompletableFuture completable future} that will complete when the whole write
   *     operation completes.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  default CompletableFuture<WriteResult> writeAsync(String statement)
      throws BulkExecutionException {
    return writeAsync(new SimpleStatement(statement));
  }

  /**
   * Executes the given write statement asynchronously.
   *
   * @param statement The statement to execute.
   * @return A {@link CompletableFuture completable future} that will complete when the whole write
   *     operation completes.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  CompletableFuture<WriteResult> writeAsync(Statement statement) throws BulkExecutionException;

  /**
   * Executes the given stream of write statements asynchronously.
   *
   * <p>This method operates in a "fire and forget" mode. If you need to consume write results,
   * consider using {@link #writeAsync(Stream, Consumer)} instead.
   *
   * @param statements The statements to execute.
   * @return A {@link CompletableFuture completable future} that will complete when the whole write
   *     operation completes.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  default CompletableFuture<Void> writeAsync(Stream<? extends Statement> statements)
      throws BulkExecutionException {
    return writeAsync(statements, ignored -> {});
  }

  /**
   * Executes the given stream of write statements asynchronously, notifying the given consumer of
   * every write result.
   *
   * @param statements The statements to execute.
   * @param consumer A consumer for write results.
   * @return A {@link CompletableFuture completable future} that will complete when the whole write
   *     operation completes.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  CompletableFuture<Void> writeAsync(
      Stream<? extends Statement> statements, Consumer<? super WriteResult> consumer)
      throws BulkExecutionException;

  /**
   * Executes the given iterable of write statements asynchronously.
   *
   * <p>This method operates in a "fire and forget" mode. If you need to consume write results,
   * consider using {@link #writeAsync(Iterable, Consumer)} instead.
   *
   * @param statements The statements to execute.
   * @return A {@link CompletableFuture completable future} that will complete when the whole write
   *     operation completes.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  default CompletableFuture<Void> writeAsync(Iterable<? extends Statement> statements)
      throws BulkExecutionException {
    return writeAsync(statements, ignored -> {});
  }

  /**
   * Executes the given iterable of write statements asynchronously, notifying the given consumer of
   * every write result.
   *
   * @param statements The statements to execute.
   * @param consumer A consumer for write results.
   * @return A {@link CompletableFuture completable future} that will complete when the whole write
   *     operation completes.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  CompletableFuture<Void> writeAsync(
      Iterable<? extends Statement> statements, Consumer<? super WriteResult> consumer)
      throws BulkExecutionException;

  /**
   * Executes the given flow of write statements asynchronously.
   *
   * <p>This method operates in a "fire and forget" mode. If you need to consume write results,
   * consider using {@link #writeAsync(Publisher, Consumer)} instead.
   *
   * @param statements The statements to execute.
   * @return A {@link CompletableFuture completable future} that will complete when the whole write
   *     operation completes.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  default CompletableFuture<Void> writeAsync(Publisher<? extends Statement> statements)
      throws BulkExecutionException {
    return writeAsync(statements, ignored -> {});
  }

  /**
   * Executes the given flow of write statements asynchronously, notifying the given consumer of
   * every write result.
   *
   * @param statements The statements to execute.
   * @param consumer A consumer for write results.
   * @return A {@link CompletableFuture completable future} that will complete when the whole write
   *     operation completes.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  CompletableFuture<Void> writeAsync(
      Publisher<? extends Statement> statements, Consumer<? super WriteResult> consumer);
}
