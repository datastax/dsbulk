/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.simulacron;

import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.dsbulk.executor.api.ContinuousReactorBulkExecutor;
import org.junit.BeforeClass;
import org.junit.Ignore;

@Ignore
public class ContinuousReactorBulkExecutorSimulacronIT extends AbstractBulkExecutorSimulacronIT {
  @BeforeClass
  public static void createBulkExecutors() {
    failFastExecutor =
        ContinuousReactorBulkExecutor.builder((ContinuousPagingSession) session).build();
    failSafeExecutor =
        ContinuousReactorBulkExecutor.builder((ContinuousPagingSession) session).failSafe().build();
  }
}
