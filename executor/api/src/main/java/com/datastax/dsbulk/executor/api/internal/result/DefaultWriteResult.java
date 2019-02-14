/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.internal.result;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class DefaultWriteResult extends DefaultResult implements WriteResult {

  @Nullable private final ResultSet rs;

  public DefaultWriteResult(@NotNull Statement statement, @NotNull ResultSet rs) {
    super(statement, rs.getExecutionInfo());
    this.rs = rs;
  }

  public DefaultWriteResult(@NotNull BulkExecutionException error) {
    super(error);
    this.rs = null;
  }

  @Override
  public boolean wasApplied() {
    return rs != null && rs.wasApplied();
  }

  @Override
  public Stream<? extends Row> getFailedWrites() {
    return rs == null || rs.wasApplied()
        ? Stream.empty()
        : StreamSupport.stream(rs.spliterator(), false);
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
