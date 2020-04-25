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
package com.datastax.oss.dsbulk.executor.api.writer;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.oss.dsbulk.executor.api.result.WriteResult;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;

/**
 * A synchronous execution unit for bulk write operations.
 *
 * <p>Methods of this interface are expected to block until the whole write operation completes.
 */
public interface SyncBulkWriter extends AutoCloseable {

  /**
   * Executes the given write statement synchronously.
   *
   * @param statement The statement to execute.
   * @return The write result.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  default WriteResult writeSync(String statement) throws BulkExecutionException {
    return writeSync(SimpleStatement.newInstance(statement));
  }

  /**
   * Executes the given write statement synchronously.
   *
   * @param statement The statement to execute.
   * @return The write result.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  WriteResult writeSync(Statement<?> statement) throws BulkExecutionException;

  /**
   * Executes the given stream of write statements synchronously.
   *
   * <p>This method operates in a "fire and forget" mode. If you need to consume write results,
   * consider using {@link #writeSync(Stream, Consumer)} instead.
   *
   * @param statements The statements to execute.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  default void writeSync(Stream<? extends Statement> statements) throws BulkExecutionException {
    writeSync(statements, ignored -> {});
  }

  /**
   * Executes the given stream of write statements synchronously, notifying the given consumer of
   * every write result.
   *
   * @param statements The statements to execute.
   * @param consumer A consumer for write results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  void writeSync(Stream<? extends Statement> statements, Consumer<? super WriteResult> consumer)
      throws BulkExecutionException;

  /**
   * Executes the given iterable of write statements synchronously.
   *
   * <p>This method operates in a "fire and forget" mode. If you need to consume write results,
   * consider using {@link #writeSync(Iterable, Consumer)} instead.
   *
   * @param statements The statements to execute.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  default void writeSync(Iterable<? extends Statement> statements) throws BulkExecutionException {
    writeSync(statements, ignored -> {});
  }

  /**
   * Executes the given iterable of write statements synchronously, notifying the given consumer of
   * every write result.
   *
   * @param statements The statements to execute.
   * @param consumer A consumer for write results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  void writeSync(Iterable<? extends Statement> statements, Consumer<? super WriteResult> consumer)
      throws BulkExecutionException;

  /**
   * Executes the given flow of write statements synchronously.
   *
   * <p>This method operates in a "fire and forget" mode. If you need to consume write results,
   * consider using {@link #writeSync(Publisher, Consumer)} instead.
   *
   * @param statements The statements to execute.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  default void writeSync(Publisher<? extends Statement> statements) throws BulkExecutionException {
    writeSync(statements, ignored -> {});
  }

  /**
   * Executes the given flow of write statements synchronously, notifying the given consumer of
   * every write result.
   *
   * @param statements The statements to execute.
   * @param consumer A consumer for write results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  void writeSync(Publisher<? extends Statement> statements, Consumer<? super WriteResult> consumer)
      throws BulkExecutionException;
}
