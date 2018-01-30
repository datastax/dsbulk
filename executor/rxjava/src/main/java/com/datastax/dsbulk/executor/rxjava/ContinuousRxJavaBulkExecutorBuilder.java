/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.rxjava;

import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.dsbulk.executor.api.AbstractBulkExecutorBuilder;

/** A builder for {@link ContinuousRxJavaBulkExecutor} instances. */
public class ContinuousRxJavaBulkExecutorBuilder
    extends AbstractBulkExecutorBuilder<ContinuousRxJavaBulkExecutor> {

  final ContinuousPagingSession continuousPagingSession;
  ContinuousPagingOptions options = ContinuousPagingOptions.builder().build();

  ContinuousRxJavaBulkExecutorBuilder(ContinuousPagingSession continuousPagingSession) {
    super(continuousPagingSession);
    this.continuousPagingSession = continuousPagingSession;
  }

  @SuppressWarnings("UnusedReturnValue")
  public AbstractBulkExecutorBuilder<ContinuousRxJavaBulkExecutor> withContinuousPagingOptions(
      ContinuousPagingOptions options) {
    this.options = options;
    return this;
  }

  @Override
  public ContinuousRxJavaBulkExecutor build() {
    return new ContinuousRxJavaBulkExecutor(this);
  }
}
