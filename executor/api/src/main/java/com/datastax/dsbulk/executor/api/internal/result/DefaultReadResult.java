/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.internal.result;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import java.util.Optional;

/** */
public final class DefaultReadResult extends DefaultResult implements ReadResult {

  private final Row row;

  public DefaultReadResult(Statement statement, ExecutionInfo executionInfo, Row row) {
    super(statement, executionInfo);
    this.row = row;
  }

  public DefaultReadResult(BulkExecutionException error) {
    super(error);
    this.row = null;
  }

  @Override
  public Optional<Row> getRow() {
    return Optional.ofNullable(row);
  }
}
