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
package com.datastax.oss.dsbulk.executor.api.result;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Common superinterface for {@link ReadResult} and {@link WriteResult}.
 *
 * <p>A {@code Result} is composed of the {@link Statement} that has been executed, and an optional
 * {@link BulkExecutionException}, if the execution failed.
 */
public interface Result {

  /**
   * Returns {@code true} if the statement execution succeeded, {@code false} otherwise.
   *
   * @return {@code true} if the statement execution succeeded, {@code false} otherwise.
   */
  default boolean isSuccess() {
    return !getError().isPresent();
  }

  /**
   * Returns the {@link Statement} that has been executed to obtain this result.
   *
   * @return the statement that has been executed.
   */
  @NonNull
  Statement<?> getStatement();

  @NonNull
  Optional<ExecutionInfo> getExecutionInfo();

  /**
   * Returns the size of the batch, if the executed statement was a {@link BatchStatement batch
   * statement}, or 1 otherwise.
   *
   * @return the size of the batch, or 1 if the statement was not a batch statement.
   */
  default int getBatchSize() {
    return getStatement() instanceof BatchStatement ? ((BatchStatement) getStatement()).size() : 1;
  }

  /**
   * Returns an optional {@link BulkExecutionException}. The value is only present if the statement
   * execution failed, in which case, this exception contains the cause of the failure.
   *
   * @return an optional {@link BulkExecutionException}.
   */
  @NonNull
  Optional<BulkExecutionException> getError();

  /**
   * If an error is present, invoke the specified consumer with the error, otherwise do nothing.
   *
   * @param consumer block to be executed if an error is present
   * @throws NullPointerException if an error is present and {@code consumer} is null
   */
  default void ifError(@NonNull Consumer<? super BulkExecutionException> consumer) {
    getError().ifPresent(consumer);
  }
}
