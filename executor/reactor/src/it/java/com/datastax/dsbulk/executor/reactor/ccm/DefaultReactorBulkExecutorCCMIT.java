/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
        DefaultReactorBulkExecutor.builder(session).build(),
        DefaultReactorBulkExecutor.builder(session).failSafe().build());
  }
}
