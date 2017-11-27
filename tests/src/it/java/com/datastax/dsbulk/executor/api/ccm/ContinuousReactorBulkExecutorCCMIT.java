/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.ccm;

import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.dsbulk.executor.reactor.ContinuousReactorBulkExecutor;
import com.datastax.dsbulk.tests.utils.VersionRequirement;
import org.junit.jupiter.api.Tag;

@Tag("ccm")
@VersionRequirement(min = "5.1")
class ContinuousReactorBulkExecutorCCMIT extends BulkExecutorCCMITBase {

  ContinuousReactorBulkExecutorCCMIT(ContinuousPagingSession session) {
    super(
        session,
        ContinuousReactorBulkExecutor.builder(session)
            // serialize execution of statements to force results to be produced in deterministic
            // order
            .withMaxInFlightRequests(1)
            .build(),
        ContinuousReactorBulkExecutor.builder(session).failSafe().build());
  }
}
