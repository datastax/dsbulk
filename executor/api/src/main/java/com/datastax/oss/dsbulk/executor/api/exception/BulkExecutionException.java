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
package com.datastax.oss.dsbulk.executor.api.exception;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.dsbulk.executor.api.BulkExecutor;

/**
 * Thrown when a {@link BulkExecutor} fails to execute a {@link Statement}.
 *
 * <p>This exception allows user code to inspect the {@link Statement} that failed, via the method
 * {@link #getStatement()}, and the cause of such failure, via the {@link #getCause()} method.
 */
public class BulkExecutionException extends RuntimeException {

  private final Statement<?> statement;

  public BulkExecutionException(Throwable cause, Statement<?> statement) {
    super(cause);
    this.statement = statement;
  }

  @Override
  public String getMessage() {
    if (statement instanceof SimpleStatement) {
      return String.format(
          "Statement execution failed: %s (%s)",
          ((SimpleStatement) statement).getQuery(), getCause().getMessage());
    } else if (statement instanceof BoundStatement) {
      return String.format(
          "Statement execution failed: %s (%s)",
          ((BoundStatement) statement).getPreparedStatement().getQuery(), getCause().getMessage());
    }
    return String.format("Statement execution failed (%s)", getCause().getMessage());
  }

  /**
   * The {@link Statement} that caused the execution failure.
   *
   * @return the {@link Statement} that caused the execution failure.
   */
  public Statement<?> getStatement() {
    return statement;
  }
}
