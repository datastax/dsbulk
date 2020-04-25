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
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Optional;
import java.util.stream.Stream;

public class EmptyWriteResult implements WriteResult {

  private final Statement<?> statement;

  public EmptyWriteResult(Statement<?> s) {
    statement = s;
  }

  @Override
  public boolean wasApplied() {
    return true;
  }

  @Override
  public Stream<? extends Row> getFailedWrites() {
    return Stream.empty();
  }

  @Override
  public @NonNull Statement<?> getStatement() {
    return statement;
  }

  @Override
  @NonNull
  public Optional<ExecutionInfo> getExecutionInfo() {
    return Optional.empty();
  }

  @Override
  @NonNull
  public Optional<BulkExecutionException> getError() {
    return Optional.empty();
  }
}
