/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.reader;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;

/**
 * An asynchronous execution unit for bulk read operations.
 *
 * <p>Methods of this interface all return {@link CompletableFuture completable futures} that will
 * complete when the whole read operation completes.
 */
public interface AsyncBulkReader extends AutoCloseable {

  /**
   * Executes the given read statement asynchronously, notifying the given consumer of every read
   * result.
   *
   * @param statement The statement to execute.
   * @param consumer A consumer for {@link ReadResult}s.
   * @return A {@link CompletableFuture completable future} that will complete when the whole read
   *     operation completes.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  default CompletableFuture<Void> readAsync(String statement, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException {
    return readAsync(new SimpleStatement(statement), consumer);
  }

  /**
   * Executes the given read statement asynchronously, notifying the given consumer of every read
   * result.
   *
   * @param statement The statement to execute.
   * @param consumer A consumer for {@link ReadResult}s.
   * @return A {@link CompletableFuture completable future} that will complete when the whole read
   *     operation completes.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  CompletableFuture<Void> readAsync(Statement statement, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException;

  /**
   * Executes the given stream of read statements asynchronously, notifying the given consumer of
   * every read result.
   *
   * @param statements The statements to execute.
   * @param consumer A consumer for {@link ReadResult}s.
   * @return A {@link CompletableFuture completable future} that will complete when the whole read
   *     operation completes.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  CompletableFuture<Void> readAsync(
      Stream<? extends Statement> statements, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException;

  /**
   * Executes the given stream of read statements asynchronously, notifying the given consumer of
   * every read result.
   *
   * @param statements The statements to execute.
   * @param consumer A consumer for {@link ReadResult}s.
   * @return A {@link CompletableFuture completable future} that will complete when the whole read
   *     operation completes.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  CompletableFuture<Void> readAsync(
      Iterable<? extends Statement> statements, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException;

  /**
   * Executes the given iterable of read statements asynchronously, notifying the given consumer of
   * every read result.
   *
   * @param statements The statements to execute.
   * @param consumer A consumer for {@link ReadResult}s.
   * @return A {@link CompletableFuture completable future} that will complete when the whole read
   *     operation completes.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  CompletableFuture<Void> readAsync(
      Publisher<? extends Statement> statements, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException;
}
