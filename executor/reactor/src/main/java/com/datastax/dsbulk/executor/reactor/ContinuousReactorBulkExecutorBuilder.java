/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.reactor;

import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.dsbulk.executor.api.AbstractBulkExecutorBuilder;

/** A builder for {@link ContinuousReactorBulkExecutor} instances. */
public class ContinuousReactorBulkExecutorBuilder
    extends AbstractBulkExecutorBuilder<ContinuousReactorBulkExecutor> {

  final ContinuousPagingSession session;
  ContinuousPagingOptions options = ContinuousPagingOptions.builder().build();

  ContinuousReactorBulkExecutorBuilder(ContinuousPagingSession session) {
    super(session);
    this.session = session;
  }

  @SuppressWarnings("UnusedReturnValue")
  public AbstractBulkExecutorBuilder<ContinuousReactorBulkExecutor> withContinuousPagingOptions(
      ContinuousPagingOptions options) {
    this.options = options;
    return this;
  }

  @Override
  public ContinuousReactorBulkExecutor build() {
    return new ContinuousReactorBulkExecutor(this);
  }
}
