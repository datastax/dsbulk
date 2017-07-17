/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.api;

import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;

/** A builder for {@link ContinuousRxJavaBulkExecutor} instances. */
public class ContinuousRxJavaBulkExecutorBuilder
    extends AbstractBulkExecutorBuilder<ContinuousRxJavaBulkExecutor> {

  private ContinuousPagingOptions options = ContinuousPagingOptions.builder().build();

  ContinuousRxJavaBulkExecutorBuilder(ContinuousPagingSession session) {
    super(session);
  }

  @SuppressWarnings("UnusedReturnValue")
  public AbstractBulkExecutorBuilder<ContinuousRxJavaBulkExecutor> withContinuousPagingOptions(
      ContinuousPagingOptions options) {
    this.options = options;
    return this;
  }

  @Override
  public ContinuousRxJavaBulkExecutor build() {
    return new ContinuousRxJavaBulkExecutor(
        (ContinuousPagingSession) session,
        options,
        failFast,
        maxInFlightRequests,
        maxRequestsPerSecond,
        listener,
        executor);
  }
}
