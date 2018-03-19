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
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.result.WriteResult;

public final class DefaultWriteResult extends DefaultResult implements WriteResult {

  public DefaultWriteResult(Statement statement, ExecutionInfo executionInfo) {
    super(statement, executionInfo);
  }

  public DefaultWriteResult(BulkExecutionException error) {
    super(error);
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
        + ']';
  }
}
