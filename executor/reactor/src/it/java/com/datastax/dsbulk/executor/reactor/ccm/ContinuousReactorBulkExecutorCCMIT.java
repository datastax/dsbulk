/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.reactor.ccm;

import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DSE;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMRequirements;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMVersionRequirement;
import com.datastax.dsbulk.executor.api.SerializedSession;
import com.datastax.dsbulk.executor.api.ccm.BulkExecutorCCMITBase;
import com.datastax.dsbulk.executor.reactor.ContinuousReactorBulkExecutor;
import com.datastax.dse.driver.api.core.DseSession;
import org.junit.jupiter.api.Tag;

@Tag("medium")
@CCMRequirements(versionRequirements = @CCMVersionRequirement(type = DSE, min = "5.1"))
class ContinuousReactorBulkExecutorCCMIT extends BulkExecutorCCMITBase {

  ContinuousReactorBulkExecutorCCMIT(CCMCluster ccm, DseSession session) {
    super(
        ccm,
        session,
        ContinuousReactorBulkExecutor.builder(new SerializedSession(session)).build(),
        ContinuousReactorBulkExecutor.builder(new SerializedSession(session)).failSafe().build());
  }
}
