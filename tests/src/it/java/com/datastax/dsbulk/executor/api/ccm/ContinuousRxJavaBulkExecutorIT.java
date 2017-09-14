/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.ccm;

import com.datastax.dsbulk.executor.api.ContinuousRxJavaBulkExecutor;
import com.datastax.dsbulk.tests.categories.LongTests;
import com.datastax.dsbulk.tests.ccm.annotations.CCMTest;
import com.datastax.dsbulk.tests.ccm.annotations.DSERequirement;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@CCMTest
@DSERequirement(min = "5.1")
@Category(LongTests.class)
public class ContinuousRxJavaBulkExecutorIT extends AbstractContinuousBulkExecutorIT {

  @BeforeClass
  public static void createBulkExecutors() {
    failFastExecutor =
        ContinuousRxJavaBulkExecutor.builder(session)
            // serialize execution of statements to force results to be produced in deterministic order
            .withMaxInFlightRequests(1)
            .build();
    failSafeExecutor = ContinuousRxJavaBulkExecutor.builder(session).failSafe().build();
  }
}
