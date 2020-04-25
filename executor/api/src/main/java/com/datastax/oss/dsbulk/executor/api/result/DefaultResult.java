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

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException;
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
