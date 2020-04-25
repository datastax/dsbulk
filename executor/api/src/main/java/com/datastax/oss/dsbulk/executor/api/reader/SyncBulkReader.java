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
package com.datastax.oss.dsbulk.executor.api.reader;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
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
