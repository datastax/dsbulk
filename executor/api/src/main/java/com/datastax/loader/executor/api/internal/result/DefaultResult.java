/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.internal.result;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Statement;
import com.datastax.loader.executor.api.exception.BulkExecutionException;
import com.datastax.loader.executor.api.result.Result;
import java.util.Optional;

/** */
abstract class DefaultResult implements Result {

  private final Statement statement;
  private final ExecutionInfo executionInfo;
  private final BulkExecutionException error;

  protected DefaultResult(Statement statement, ExecutionInfo executionInfo) {
    this.statement = statement;
    this.executionInfo = executionInfo;
    this.error = null;
  }

  protected DefaultResult(BulkExecutionException error) {
    this.statement = error.getStatement();
    this.error = error;
    this.executionInfo = null;
  }

  @Override
  public boolean isSuccess() {
    return error == null;
  }

  @Override
  public Statement getStatement() {
    return statement;
  }

  @Override
  public Optional<BulkExecutionException> getError() {
    return Optional.ofNullable(error);
  }

  @Override
  public Optional<ExecutionInfo> getExecutionInfo() {
    return Optional.ofNullable(executionInfo);
  }
}
