/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.simulacron;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.executor.reactor.DefaultReactorBulkExecutor;
import com.datastax.oss.simulacron.server.BoundCluster;

class DefaultReactorBulkExecutorSimulacronIT extends BulkExecutorSimulacronITBase {

  DefaultReactorBulkExecutorSimulacronIT(BoundCluster simulacron, Session session) {
    super(
        simulacron,
        session,
        DefaultReactorBulkExecutor.builder(session)
            // serialize execution of statements to force results to be produced in deterministic
            // order
            .withMaxInFlightRequests(1)
            .build(),
        DefaultReactorBulkExecutor.builder(session).failSafe().build());
  }
}
