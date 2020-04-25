/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.executor.reactor.ccm;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.dsbulk.tests.driver.SerializedSession;
import com.datastax.oss.dsbulk.executor.api.ccm.BulkExecutorCCMITBase;
import com.datastax.oss.dsbulk.executor.reactor.ContinuousReactorBulkExecutor;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Type;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMRequirements;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMVersionRequirement;
import org.junit.jupiter.api.Tag;

@Tag("medium")
@CCMRequirements(
    compatibleTypes = {Type.DSE},
    versionRequirements = @CCMVersionRequirement(type = Type.DSE, min = "5.1"))
class ContinuousReactorBulkExecutorCCMIT extends BulkExecutorCCMITBase {

  ContinuousReactorBulkExecutorCCMIT(CCMCluster ccm, CqlSession session) {
    super(
        ccm,
        session,
        ContinuousReactorBulkExecutor.continuousPagingBuilder(new SerializedSession(session))
            .build(),
        ContinuousReactorBulkExecutor.continuousPagingBuilder(new SerializedSession(session))
            .failSafe()
            .build());
  }
}
