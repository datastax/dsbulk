/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api;

import com.datastax.loader.tests.ccm.annotations.CCMTest;
import org.junit.BeforeClass;

@CCMTest
public class ContinuousRxJavaBulkExecutorIT extends AbstractContinuousBulkExecutorIT {

  @BeforeClass
  public static void createBulkExecutors() {
    failFastExecutor = ContinuousRxJavaBulkExecutor.builder(session).build();
    failSafeExecutor =
        ContinuousRxJavaBulkExecutor.builder(session).failSafe().build();
  }

}
