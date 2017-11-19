/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.simulacron;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.executor.rxjava.DefaultRxJavaBulkExecutor;
import com.datastax.oss.simulacron.server.BoundCluster;

class DefaultRxJavaBulkExecutorSimulacronIT extends BulkExecutorSimulacronITBase {

  DefaultRxJavaBulkExecutorSimulacronIT(BoundCluster simulacron, Session session) {
    super(
        simulacron,
        session,
        DefaultRxJavaBulkExecutor.builder(session)
            // serialize execution of statements to force results to be produced in deterministic
            // order
            .withMaxInFlightRequests(1)
            .build(),
        DefaultRxJavaBulkExecutor.builder(session).failSafe().build());
  }
}
