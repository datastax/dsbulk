/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.reactor.ccm;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.driver.annotations.SessionConfig;
import com.datastax.dsbulk.executor.api.SerializedSession;
import com.datastax.dsbulk.executor.api.ccm.BulkExecutorCCMITBase;
import com.datastax.dsbulk.executor.reactor.DefaultReactorBulkExecutor;
import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.Tag;

@Tag("medium")
class DefaultReactorBulkExecutorCCMIT extends BulkExecutorCCMITBase {

  DefaultReactorBulkExecutorCCMIT(
      CCMCluster ccm,
      @SessionConfig(settings = "basic.request.page-size=10" /* to force pagination */)
          CqlSession session) {
    super(
        ccm,
        session,
        DefaultReactorBulkExecutor.builder(new SerializedSession(session)).build(),
        DefaultReactorBulkExecutor.builder(new SerializedSession(session)).failSafe().build());
  }
}
