/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import reactor.util.concurrent.Queues;

/** A builder for {@link DefaultReactorBulkExecutor} instances. */
public class DefaultReactorBulkExecutorBuilder
    extends AbstractBulkExecutorBuilder<DefaultReactorBulkExecutor> {

  DefaultReactorBulkExecutorBuilder(Session session) {
    super(session);
  }

  @Override
  public DefaultReactorBulkExecutor build() {
    if (queueFactory == null) {
      queueFactory = statement -> Queues.<ReadResult>get(statement.getFetchSize() * 4).get();
    }
    return new DefaultReactorBulkExecutor(
        session,
        failFast,
        maxInFlightRequests,
        maxRequestsPerSecond,
        listener,
        executor,
        queueFactory);
  }
}
