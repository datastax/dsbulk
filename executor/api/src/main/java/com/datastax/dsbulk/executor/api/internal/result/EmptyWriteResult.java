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
import com.datastax.dsbulk.executor.api.result.WriteResult;
import java.util.Optional;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;

public class EmptyWriteResult implements WriteResult {

  private final Statement statement;

  public EmptyWriteResult(Statement s) {
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
  @NotNull
  public Statement getStatement() {
    return statement;
  }

  @Override
  @NotNull
  public Optional<ExecutionInfo> getExecutionInfo() {
    return Optional.empty();
  }

  @Override
  @NotNull
  public Optional<BulkExecutionException> getError() {
    return Optional.empty();
  }
}
