/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.reactor;

import com.datastax.dsbulk.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.dsbulk.executor.api.BulkExecutor;
import com.datastax.dsbulk.executor.api.NonContinuousBulkExecutorTestBase;

public class DefaultReactorBulkExecutorTest extends NonContinuousBulkExecutorTestBase {

  @Override
  protected BulkExecutor newBulkExecutor(boolean failSafe) {
    AbstractBulkExecutorBuilder<DefaultReactorBulkExecutor> builder =
        DefaultReactorBulkExecutor.builder(session).withExecutionListener(listener);
    if (failSafe) {
      builder.failSafe();
    }
    return builder.build();
  }
}
