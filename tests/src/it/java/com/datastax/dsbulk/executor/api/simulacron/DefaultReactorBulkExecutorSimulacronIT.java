/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.simulacron;

import com.datastax.dsbulk.executor.reactor.DefaultReactorBulkExecutor;
import org.junit.BeforeClass;

public class DefaultReactorBulkExecutorSimulacronIT extends AbstractBulkExecutorSimulacronIT {

  @BeforeClass
  public static void createBulkExecutors() {
    failFastExecutor =
        DefaultReactorBulkExecutor.builder(session)
            // serialize execution of statements to force results to be produced in deterministic
            // order
            .withMaxInFlightRequests(1)
            .build();
    failSafeExecutor = DefaultReactorBulkExecutor.builder(session).failSafe().build();
  }
}
