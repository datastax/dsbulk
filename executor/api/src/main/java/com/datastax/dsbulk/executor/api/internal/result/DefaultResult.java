/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.internal.result;

import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.result.Result;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Statement;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.Optional;

abstract class DefaultResult implements Result {

  private final Statement<?> statement;
  private final ExecutionInfo executionInfo;
  private final BulkExecutionException error;

  protected DefaultResult(@NonNull Statement<?> statement, @NonNull ExecutionInfo executionInfo) {
    this.statement = statement;
    this.executionInfo = executionInfo;
    this.error = null;
  }

  protected DefaultResult(@NonNull BulkExecutionException error) {
    this.statement = error.getStatement();
    this.error = error;
    this.executionInfo = null;
  }

  @Override
  public boolean isSuccess() {
    return error == null;
  }

  @Override
  public @NonNull Statement<?> getStatement() {
    return statement;
  }

  @NonNull
  @Override
  public Optional<BulkExecutionException> getError() {
    return Optional.ofNullable(error);
  }

  @NonNull
  @Override
  public Optional<ExecutionInfo> getExecutionInfo() {
    return Optional.ofNullable(executionInfo);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DefaultResult)) {
      return false;
    }
    DefaultResult that = (DefaultResult) o;
    return statement.equals(that.statement)
        && Objects.equals(executionInfo, that.executionInfo)
        && Objects.equals(error, that.error);
  }

  @Override
  public int hashCode() {
    int result = statement.hashCode();
    result = 31 * result + (executionInfo != null ? executionInfo.hashCode() : 0);
    result = 31 * result + (error != null ? error.hashCode() : 0);
    return result;
  }
}
