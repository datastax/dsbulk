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
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
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
