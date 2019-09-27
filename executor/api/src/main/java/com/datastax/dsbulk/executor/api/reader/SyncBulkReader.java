/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.reader;

import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
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
    readSync(SimpleStatement.newInstance(statement), consumer);
  }

  /**
   * Executes the given read statement synchronously, notifying the given consumer of every read
   * result.
   *
   * @param statement The statement to execute.
   * @param consumer A consumer for read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  void readSync(Statement<?> statement, Consumer<? super ReadResult> consumer)
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
