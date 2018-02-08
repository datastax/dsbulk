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
import com.datastax.driver.core.SerializedSession;
import com.datastax.dsbulk.commons.tests.driver.annotations.ClusterConfig;
import com.datastax.dsbulk.executor.api.ccm.BulkExecutorCCMITBase;
import com.datastax.dsbulk.executor.rxjava.DefaultRxJavaBulkExecutor;
import org.junit.jupiter.api.Tag;

@Tag("ccm")
class DefaultRxJavaBulkExecutorCCMIT extends BulkExecutorCCMITBase {

  DefaultRxJavaBulkExecutorCCMIT(
      @ClusterConfig(queryOptions = "fetchSize:100" /* to force pagination */)
          ContinuousPagingSession session) {
    super(
        session,
        DefaultRxJavaBulkExecutor.builder(new SerializedSession(session)).build(),
        DefaultRxJavaBulkExecutor.builder(new SerializedSession(session)).failSafe().build());
  }
}
