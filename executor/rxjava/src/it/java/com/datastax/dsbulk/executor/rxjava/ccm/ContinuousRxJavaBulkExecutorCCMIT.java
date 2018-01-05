/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.rxjava.ccm;

import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.dsbulk.commons.tests.utils.VersionRequirement;
import com.datastax.dsbulk.executor.api.ccm.BulkExecutorCCMITBase;
import com.datastax.dsbulk.executor.rxjava.ContinuousRxJavaBulkExecutor;
import org.junit.jupiter.api.Tag;

@Tag("ccm")
@VersionRequirement(min = "5.1")
class ContinuousRxJavaBulkExecutorCCMIT extends BulkExecutorCCMITBase {

  ContinuousRxJavaBulkExecutorCCMIT(ContinuousPagingSession session) {
    super(
        session,
        ContinuousRxJavaBulkExecutor.builder(session)
            // serialize execution of statements to force results to be produced in deterministic
            // order
            .withMaxInFlightRequests(1)
            .build(),
        ContinuousRxJavaBulkExecutor.builder(session).failSafe().build());
  }
}
