/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api;

import com.google.common.util.concurrent.MoreExecutors;

public class DefaultReactorBulkExecutorTest extends AbstractNonContinuousBulkExecutorTest {

  @Override
  protected BulkExecutor newBulkExecutor(boolean failSafe) {
    AbstractBulkExecutorBuilder<DefaultReactorBulkExecutor> builder =
        DefaultReactorBulkExecutor.builder(session)
            // serialize execution of statements to force results to be produced in deterministic order
            .withExecutor(MoreExecutors.directExecutor())
            .withExecutionListener(listener);
    if (failSafe) builder.failSafe();
    return builder.build();
  }
}
