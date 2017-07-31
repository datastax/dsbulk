/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.ccm;

import com.datastax.loader.executor.api.DefaultRxJavaBulkExecutor;
import com.datastax.loader.tests.ccm.annotations.CCMTest;
import org.junit.BeforeClass;

@CCMTest
public class DefaultRxJavaBulkExecutorIT extends AbstractBulkExecutorIT {

  @BeforeClass
  public static void createBulkExecutors() {
    failFastExecutor = DefaultRxJavaBulkExecutor.builder(session).build();
    failSafeExecutor =
        DefaultRxJavaBulkExecutor.builder(session).failSafe().build();
  }

}

