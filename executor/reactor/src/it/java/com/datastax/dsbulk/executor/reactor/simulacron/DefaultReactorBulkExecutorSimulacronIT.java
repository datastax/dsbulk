/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.reactor.simulacron;

import com.datastax.dsbulk.executor.api.SerializedSession;
import com.datastax.dsbulk.executor.api.simulacron.BulkExecutorSimulacronITBase;
import com.datastax.dsbulk.executor.reactor.DefaultReactorBulkExecutor;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.simulacron.server.BoundCluster;

class DefaultReactorBulkExecutorSimulacronIT extends BulkExecutorSimulacronITBase {

  DefaultReactorBulkExecutorSimulacronIT(BoundCluster simulacron, CqlSession session) {
    super(
        simulacron,
        DefaultReactorBulkExecutor.builder(new SerializedSession(session)).build(),
        DefaultReactorBulkExecutor.builder(new SerializedSession(session)).failSafe().build());
  }
}
