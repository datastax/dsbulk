/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.rxjava.ccm;

import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DSE;

import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.SerializedSession;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMRequirements;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMVersionRequirement;
import com.datastax.dsbulk.executor.api.ccm.BulkExecutorCCMITBase;
import com.datastax.dsbulk.executor.rxjava.ContinuousRxJavaBulkExecutor;
import org.junit.jupiter.api.Tag;

@Tag("medium")
@CCMRequirements(
  compatibleTypes = DSE,
  versionRequirements = @CCMVersionRequirement(type = DSE, min = "5.1")
)
class ContinuousRxJavaBulkExecutorCCMIT extends BulkExecutorCCMITBase {

  ContinuousRxJavaBulkExecutorCCMIT(ContinuousPagingSession session) {
    super(
        session,
        ContinuousRxJavaBulkExecutor.builder(new SerializedSession(session)).build(),
        ContinuousRxJavaBulkExecutor.builder(new SerializedSession(session)).failSafe().build());
  }
}
