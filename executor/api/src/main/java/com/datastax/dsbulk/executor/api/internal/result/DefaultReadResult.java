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
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import java.util.Optional;

public final class DefaultReadResult extends DefaultResult implements ReadResult {

  private final Row row;

  public DefaultReadResult(Statement statement, ExecutionInfo executionInfo, Row row) {
    super(statement, executionInfo);
    this.row = row;
  }

  public DefaultReadResult(BulkExecutionException error) {
    super(error);
    row = null;
  }

  @Override
  public Optional<Row> getRow() {
    return Optional.ofNullable(row);
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
    return row != null ? row.equals(that.row) : that.row == null;
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
