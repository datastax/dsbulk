/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api;

import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import reactor.util.concurrent.Queues;

/** A builder for {@link ContinuousReactorBulkExecutor} instances. */
public class ContinuousReactorBulkExecutorBuilder
    extends AbstractBulkExecutorBuilder<ContinuousReactorBulkExecutor> {

  private ContinuousPagingOptions options = ContinuousPagingOptions.builder().build();

  ContinuousReactorBulkExecutorBuilder(ContinuousPagingSession session) {
    super(session);
  }

  @SuppressWarnings("UnusedReturnValue")
  public AbstractBulkExecutorBuilder<ContinuousReactorBulkExecutor> withContinuousPagingOptions(
      ContinuousPagingOptions options) {
    this.options = options;
    return this;
  }

  @Override
  public ContinuousReactorBulkExecutor build() {
    if (queueFactory == null) {
      queueFactory = statement -> Queues.<ReadResult>get(options.getPageSize() * 4).get();
    }
    return new ContinuousReactorBulkExecutor(
        (ContinuousPagingSession) session,
        options,
        failFast,
        maxInFlightRequests,
        maxRequestsPerSecond,
        listener,
        executor,
        queueFactory);
  }
}
