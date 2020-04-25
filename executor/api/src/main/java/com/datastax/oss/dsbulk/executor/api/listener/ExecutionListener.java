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
package com.datastax.oss.dsbulk.executor.api.listener;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.dsbulk.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException;

/**
 * A listener for bulk executions.
 *
 * @see AbstractBulkExecutorBuilder#withExecutionListener(ExecutionListener)
 */
public interface ExecutionListener {

  /**
   * Called when a statement is about to be executed.
   *
   * @param statement the statement to execute.
   * @param context the global statement execution context.
   */
  default void onExecutionStarted(Statement<?> statement, ExecutionContext context) {}

  /**
   * Called when a write request is about to be sent.
   *
   * @param statement the statement to execute.
   * @param context the local request execution context.
   */
  default void onWriteRequestStarted(Statement<?> statement, ExecutionContext context) {}

  /**
   * Called when a write request has been completed successfully.
   *
   * @param statement the statement to execute.
   * @param context the local request execution context.
   */
  default void onWriteRequestSuccessful(Statement<?> statement, ExecutionContext context) {}

  /**
   * Called when a write request has failed.
   *
   * @param statement the statement to execute.
   * @param error the request execution error.
   * @param context the local request execution context.
   */
  default void onWriteRequestFailed(
      Statement<?> statement, Throwable error, ExecutionContext context) {}

  /**
   * Called when a read request is about to be sent.
   *
   * @param statement the statement to execute.
   * @param context the local request execution context.
   */
  default void onReadRequestStarted(Statement<?> statement, ExecutionContext context) {}

  /**
   * Called when a read request has been completed successfully.
   *
   * @param statement the statement to execute.
   * @param context the local request execution context.
   */
  default void onReadRequestSuccessful(Statement<?> statement, ExecutionContext context) {}

  /**
   * Called when a row has been successfully received. Applicable only for reads.
   *
   * <p>Note: this method is called when the row is effectively read and emitted to downstream
   * subscribers, which might be later than the moment when it was made available by the driver.
   *
   * @param row the row.
   * @param context the local request execution context.
   */
  default void onRowReceived(Row row, ExecutionContext context) {}

  /**
   * Called when a read request has failed.
   *
   * @param statement the statement to execute.
   * @param error the request execution error.
   * @param context the local request execution context.
   */
  default void onReadRequestFailed(
      Statement<?> statement, Throwable error, ExecutionContext context) {}

  /**
   * Called when a statement has been successfully executed.
   *
   * @param statement the executed statement.
   * @param context the global statement execution context.
   */
  default void onExecutionSuccessful(Statement<?> statement, ExecutionContext context) {}

  /**
   * Called when a statement execution has failed.
   *
   * @param exception the error encountered.
   * @param context the statement execution context.
   */
  default void onExecutionFailed(BulkExecutionException exception, ExecutionContext context) {}
}
