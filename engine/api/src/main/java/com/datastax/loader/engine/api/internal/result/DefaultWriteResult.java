/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.api.internal.result;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.loader.engine.api.exception.BulkExecutionException;
import com.datastax.loader.engine.api.result.WriteResult;
import java.util.Optional;

/** */
public final class DefaultWriteResult extends DefaultResult implements WriteResult {

  private final ResultSet rs;

  public DefaultWriteResult(Statement statement, ResultSet rs) {
    super(statement);
    this.rs = rs;
  }

  public DefaultWriteResult(BulkExecutionException error) {
    super(error);
    this.rs = null;
  }

  @Override
  public Optional<ResultSet> getResultSet() {
    return Optional.ofNullable(rs);
  }
}
