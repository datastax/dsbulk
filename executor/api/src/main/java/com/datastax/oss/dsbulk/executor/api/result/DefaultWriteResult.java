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

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class DefaultWriteResult extends DefaultResult implements WriteResult {

  @Nullable private final AsyncResultSet rs;
  private boolean wasApplied;

  public DefaultWriteResult(@NonNull Statement<?> statement, @NonNull AsyncResultSet rs) {
    super(statement, rs.getExecutionInfo());
    this.rs = rs;
    wasApplied = rs.wasApplied();
  }

  public DefaultWriteResult(@NonNull BulkExecutionException error) {
    super(error);
    this.rs = null;
    wasApplied = false;
  }

  @Override
  public boolean wasApplied() {
    return wasApplied;
  }

  @Override
  public Stream<? extends Row> getFailedWrites() {
    return rs == null || wasApplied
        ? Stream.empty()
        : StreamSupport.stream(rs.currentPage().spliterator(), false);
  }

  @Override
  public String toString() {
    return "DefaultWriteResult["
        + "error="
        + getError()
        + ", statement="
        + getStatement()
        + ", executionInfo="
        + getExecutionInfo()
        + ", wasApplied="
        + wasApplied()
        + ']';
  }
}
