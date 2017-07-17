/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.api;

import com.datastax.driver.core.Session;

/** A builder for {@link DefaultReactorBulkExecutor} instances. */
public class DefaultReactorBulkExecutorBuilder
    extends AbstractBulkExecutorBuilder<DefaultReactorBulkExecutor> {

  DefaultReactorBulkExecutorBuilder(Session session) {
    super(session);
  }

  @Override
  public DefaultReactorBulkExecutor build() {
    return new DefaultReactorBulkExecutor(
        session, failFast, maxInFlightRequests, maxRequestsPerSecond, listener, executor);
  }
}
