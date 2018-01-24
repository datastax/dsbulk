/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.exception;

import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.BulkExecutor;

/**
 * Thrown when a {@link BulkExecutor} fails to execute a {@link Statement}.
 *
 * <p>This exception allows user code to inspect the {@link Statement} that failed, via the method
 * {@link #getStatement()}, and the cause of such failure, via the {@link #getCause()} method.
 */
public class BulkExecutionException extends RuntimeException {

  private final Statement statement;

  public BulkExecutionException(Throwable cause, Statement statement) {
    super(cause);
    this.statement = statement;
  }

  @Override
  public String getMessage() {
    return String.format("Statement execution failed: %s (%s)", statement, getCause().getMessage());
  }

  /**
   * The {@link Statement} that caused the execution failure.
   *
   * @return the {@link Statement} that caused the execution failure.
   */
  public Statement getStatement() {
    return statement;
  }
}
