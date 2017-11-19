/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.ccm;

import com.datastax.dsbulk.executor.rxjava.DefaultRxJavaBulkExecutor;
import com.datastax.dsbulk.tests.categories.LongTests;
import com.datastax.dsbulk.tests.ccm.annotations.CCMTest;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@CCMTest
@Category(LongTests.class)
public class DefaultRxJavaBulkExecutorIT extends AbstractBulkExecutorIT {

  @BeforeClass
  public static void createBulkExecutors() {
    failFastExecutor =
        DefaultRxJavaBulkExecutor.builder(session)
            // serialize execution of statements to force results to be produced in deterministic
            // order
            .withMaxInFlightRequests(1)
            .build();
    failSafeExecutor = DefaultRxJavaBulkExecutor.builder(session).failSafe().build();
  }
}
