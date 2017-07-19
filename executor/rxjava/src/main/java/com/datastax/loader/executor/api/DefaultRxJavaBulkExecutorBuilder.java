/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api;

import com.datastax.driver.core.Session;

/** A builder for {@link DefaultRxJavaBulkExecutor} instances. */
public class DefaultRxJavaBulkExecutorBuilder
    extends AbstractBulkExecutorBuilder<DefaultRxJavaBulkExecutor> {

  DefaultRxJavaBulkExecutorBuilder(Session session) {
    super(session);
  }

  @Override
  public DefaultRxJavaBulkExecutor build() {
    return new DefaultRxJavaBulkExecutor(
        session, failFast, maxInFlightRequests, maxRequestsPerSecond, listener, executor);
  }
}
