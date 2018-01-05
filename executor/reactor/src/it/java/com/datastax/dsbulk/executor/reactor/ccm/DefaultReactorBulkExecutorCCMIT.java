/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.reactor.ccm;

import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.dsbulk.commons.tests.driver.annotations.ClusterConfig;
import com.datastax.dsbulk.executor.api.ccm.BulkExecutorCCMITBase;
import com.datastax.dsbulk.executor.reactor.DefaultReactorBulkExecutor;
import org.junit.jupiter.api.Tag;

@Tag("ccm")
class DefaultReactorBulkExecutorCCMIT extends BulkExecutorCCMITBase {

  DefaultReactorBulkExecutorCCMIT(
      @ClusterConfig(queryOptions = "fetchSize:100" /* to force pagination */)
          ContinuousPagingSession session) {
    super(
        session,
        DefaultReactorBulkExecutor.builder(session)
            // serialize execution of statements to force results to be produced in deterministic
            // order
            .withMaxInFlightRequests(1)
            .build(),
        DefaultReactorBulkExecutor.builder(session).failSafe().build());
  }
}
