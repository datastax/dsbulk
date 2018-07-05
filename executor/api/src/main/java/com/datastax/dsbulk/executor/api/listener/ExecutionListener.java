/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.listener;

import com.datastax.dsbulk.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;

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
  default void onExecutionStarted(Statement statement, ExecutionContext context) {}

  /**
   * Called when a write request is about to be sent.
   *
   * @param statement the statement to execute.
   * @param context the local request execution context.
   */
  default void onWriteRequestStarted(Statement statement, ExecutionContext context) {}

  /**
   * Called when a write request has been completed successfully.
   *
   * @param statement the statement to execute.
   * @param context the local request execution context.
   */
  default void onWriteRequestSuccessful(Statement statement, ExecutionContext context) {}

  /**
   * Called when a write request has failed.
   *
   * @param statement the statement to execute.
   * @param error the request execution error.
   * @param context the local request execution context.
   */
  default void onWriteRequestFailed(
      Statement statement, Throwable error, ExecutionContext context) {}

  /**
   * Called when a read request is about to be sent.
   *
   * @param statement the statement to execute.
   * @param context the local request execution context.
   */
  default void onReadRequestStarted(Statement statement, ExecutionContext context) {}

  /**
   * Called when a read request has been completed successfully.
   *
   * @param statement the statement to execute.
   * @param context the local request execution context.
   */
  default void onReadRequestSuccessful(Statement statement, ExecutionContext context) {}

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
      Statement statement, Throwable error, ExecutionContext context) {}

  /**
   * Called when a statement has been successfully executed.
   *
   * @param statement the executed statement.
   * @param context the global statement execution context.
   */
  default void onExecutionSuccessful(Statement statement, ExecutionContext context) {}

  /**
   * Called when a statement execution has failed.
   *
   * @param exception the error encountered.
   * @param context the statement execution context.
   */
  default void onExecutionFailed(BulkExecutionException exception, ExecutionContext context) {}
}
