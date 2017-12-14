/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.rxjava.ccm;

import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.dsbulk.executor.api.ccm.BulkExecutorCCMITBase;
import com.datastax.dsbulk.executor.rxjava.DefaultRxJavaBulkExecutor;
import com.datastax.dsbulk.tests.driver.annotations.ClusterConfig;
import org.junit.jupiter.api.Tag;

@Tag("ccm")
class DefaultRxJavaBulkExecutorCCMIT extends BulkExecutorCCMITBase {

  DefaultRxJavaBulkExecutorCCMIT(
      @ClusterConfig(queryOptions = "fetchSize:100" /* to force pagination */)
          ContinuousPagingSession session) {
    super(
        session,
        DefaultRxJavaBulkExecutor.builder(session)
            // serialize execution of statements to force results to be produced in deterministic
            // order
            .withMaxInFlightRequests(1)
            .build(),
        DefaultRxJavaBulkExecutor.builder(session).failSafe().build());
  }
}
