/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.rxjava;

import com.datastax.dsbulk.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.dsbulk.executor.api.AbstractNonContinuousBulkExecutorTest;
import com.datastax.dsbulk.executor.api.BulkExecutor;

public class DefaultRxJavaBulkExecutorTest extends AbstractNonContinuousBulkExecutorTest {

  @Override
  protected BulkExecutor newBulkExecutor(boolean failSafe) {
    AbstractBulkExecutorBuilder<DefaultRxJavaBulkExecutor> builder =
        DefaultRxJavaBulkExecutor.builder(session)
            // serialize execution of statements to force results to be produced in deterministic order
            .withoutExecutor()
            .withExecutionListener(listener);
    if (failSafe) {
      builder.failSafe();
    }
    return builder.build();
  }
}
