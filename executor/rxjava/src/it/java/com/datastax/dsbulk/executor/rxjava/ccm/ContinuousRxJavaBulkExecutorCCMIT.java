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

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMRequirements;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMVersionRequirement;
import com.datastax.dsbulk.executor.api.SerializedSession;
import com.datastax.dsbulk.executor.api.ccm.BulkExecutorCCMITBase;
import com.datastax.dsbulk.executor.rxjava.ContinuousRxJavaBulkExecutor;
import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.Tag;

@Tag("medium")
@CCMRequirements(
    compatibleTypes = {DSE},
    versionRequirements = @CCMVersionRequirement(type = DSE, min = "5.1"))
class ContinuousRxJavaBulkExecutorCCMIT extends BulkExecutorCCMITBase {

  ContinuousRxJavaBulkExecutorCCMIT(CCMCluster ccm, CqlSession session) {
    super(
        ccm,
        session,
        ContinuousRxJavaBulkExecutor.builderCP(new SerializedSession(session)).build(),
        ContinuousRxJavaBulkExecutor.builderCP(new SerializedSession(session)).failSafe().build());
  }
}
