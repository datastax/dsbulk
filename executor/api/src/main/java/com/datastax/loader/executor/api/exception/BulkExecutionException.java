/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.exception;

import com.datastax.driver.core.Statement;
import com.datastax.loader.executor.api.BulkExecutor;

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
