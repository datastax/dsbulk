/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.reader;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.loader.executor.api.exception.BulkExecutionException;
import com.datastax.loader.executor.api.result.ReadResult;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;

/**
 * An asynchronous execution unit for bulk read operations.
 *
 * <p>Methods of this interface are expected to block until the whole read operation completes.
 */
public interface SyncBulkReader extends AutoCloseable {

  /**
   * Executes the given read statement synchronously, notifying the given consumer of every read
   * result.
   *
   * @param statement The statement to execute.
   * @param consumer A consumer for read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  default void readSync(String statement, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException {
    readSync(new SimpleStatement(statement), consumer);
  }

  /**
   * Executes the given read statement synchronously, notifying the given consumer of every read
   * result.
   *
   * @param statement The statement to execute.
   * @param consumer A consumer for read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  void readSync(Statement statement, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException;

  /**
   * Executes the given stream of read statements synchronously, notifying the given consumer of
   * every read result.
   *
   * @param statements The statements to execute.
   * @param consumer A consumer for read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  void readSync(Stream<? extends Statement> statements, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException;

  /**
   * Executes the given stream of read statements synchronously, notifying the given consumer of
   * every read result.
   *
   * @param statements The statements to execute.
   * @param consumer A consumer for read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  void readSync(Iterable<? extends Statement> statements, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException;

  /**
   * Executes the given iterable of read statements asynchronously, notifying the given consumer of
   * every read result.
   *
   * @param statements The statements to execute.
   * @param consumer A consumer for read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  void readSync(Publisher<? extends Statement> statements, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException;
}
