/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.listener;

import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.result.Result;

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
   * @param context the statement execution context.
   */
  default void onExecutionStarted(Statement statement, ExecutionContext context) {}

  /**
   * Called when a result is received.
   *
   * @param result the received result.
   * @param context the statement execution context.
   */
  default void onResultReceived(Result result, ExecutionContext context) {}

  /**
   * Called when a statement has been successfully executed.
   *
   * @param statement the executed statement.
   * @param context the statement execution context.
   */
  default void onExecutionCompleted(Statement statement, ExecutionContext context) {}

  /**
   * Called when a statement execution has failed.
   *
   * @param exception the error encountered.
   * @param context the statement execution context.
   */
  default void onExecutionFailed(BulkExecutionException exception, ExecutionContext context) {}
}
