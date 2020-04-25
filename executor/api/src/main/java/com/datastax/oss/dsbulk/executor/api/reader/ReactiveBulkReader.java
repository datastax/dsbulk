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
import java.util.stream.Stream;
import org.reactivestreams.Publisher;

/**
 * A reactive execution unit for bulk read operations.
 *
 * <p>Methods of this interface all return a {@link Publisher} of read results.
 */
public interface ReactiveBulkReader extends AutoCloseable {

  /**
   * Executes the given read statement reactively.
   *
   * @param statement The statement to execute.
   * @return A {@link Publisher publisher} that will emit one single read result.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  default Publisher<ReadResult> readReactive(String statement) throws BulkExecutionException {
    return readReactive(SimpleStatement.newInstance(statement));
  }

  /**
   * Executes the given read statement reactively.
   *
   * @param statement The statement to execute.
   * @return A {@link Publisher publisher} that will emit one single read result.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Publisher<ReadResult> readReactive(Statement<?> statement) throws BulkExecutionException;

  /**
   * Executes the given stream of read statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Publisher publisher} of read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Publisher<ReadResult> readReactive(Stream<? extends Statement> statements)
      throws BulkExecutionException;

  /**
   * Executes the given iterable of read statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Publisher publisher} of read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Publisher<ReadResult> readReactive(Iterable<? extends Statement> statements)
      throws BulkExecutionException;

  /**
   * Executes the given publisher of read statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Publisher publisher} of read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Publisher<ReadResult> readReactive(Publisher<? extends Statement> statements)
      throws BulkExecutionException;
}
