/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.internal.result;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.result.WriteResult;

/** */
public final class DefaultWriteResult extends DefaultResult implements WriteResult {

  public DefaultWriteResult(Statement statement, ExecutionInfo executionInfo) {
    super(statement, executionInfo);
  }

  public DefaultWriteResult(BulkExecutionException error) {
    super(error);
  }
}
