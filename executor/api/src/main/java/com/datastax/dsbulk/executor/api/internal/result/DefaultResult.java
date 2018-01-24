/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.internal.result;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.result.Result;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DefaultResult that = (DefaultResult) o;
    return statement.equals(that.statement)
        && (executionInfo != null
            ? executionInfo.equals(that.executionInfo)
            : that.executionInfo == null)
        && (error != null ? error.equals(that.error) : that.error == null);
  }

  @Override
  public int hashCode() {
    int result = statement.hashCode();
    result = 31 * result + (executionInfo != null ? executionInfo.hashCode() : 0);
    result = 31 * result + (error != null ? error.hashCode() : 0);
    return result;
  }
}
