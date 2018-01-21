/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
        ContinuousRxJavaBulkExecutor.builder(session).build(),
        ContinuousRxJavaBulkExecutor.builder(session).failSafe().build());
  }
}
