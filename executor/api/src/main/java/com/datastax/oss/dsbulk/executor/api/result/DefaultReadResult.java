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
import java.util.Objects;
import java.util.Optional;

public final class DefaultReadResult extends DefaultResult implements ReadResult {

  private final Row row;
  private final long position;

  public DefaultReadResult(
      @NonNull Statement<?> statement,
      @NonNull ExecutionInfo executionInfo,
      @NonNull Row row,
      long position) {
    super(statement, executionInfo);
    this.row = row;
    this.position = position;
  }

  public DefaultReadResult(@NonNull BulkExecutionException error) {
    super(error);
    row = null;
    position = -1;
  }

  @NonNull
  @Override
  public Optional<Row> getRow() {
    return Optional.ofNullable(row);
  }

  @Override
  public long getPosition() {
    return position;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    DefaultReadResult that = (DefaultReadResult) o;
    return Objects.equals(row, that.row);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (row != null ? row.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "DefaultReadResult["
        + "row="
        + getRow()
        + ", error="
        + getError()
        + ", statement="
        + getStatement()
        + ", executionInfo="
        + getExecutionInfo()
        + ']';
  }
}
