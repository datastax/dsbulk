/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.reactor;

import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.dsbulk.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.dsbulk.executor.api.QueueFactory;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import reactor.util.concurrent.Queues;

/** A builder for {@link ContinuousReactorBulkExecutor} instances. */
public class ContinuousReactorBulkExecutorBuilder
    extends AbstractBulkExecutorBuilder<ContinuousReactorBulkExecutor> {

  final ContinuousPagingSession session;
  ContinuousPagingOptions options = ContinuousPagingOptions.builder().build();

  ContinuousReactorBulkExecutorBuilder(ContinuousPagingSession session) {
    super(session);
    this.session = session;
    queueFactory = statement -> Queues.<ReadResult>get(options.getPageSize() * 4).get();
  }

  @SuppressWarnings("UnusedReturnValue")
  public AbstractBulkExecutorBuilder<ContinuousReactorBulkExecutor> withContinuousPagingOptions(
      ContinuousPagingOptions options) {
    this.options = options;
    return this;
  }

  /**
   * Sets the {@link QueueFactory} to use when executing read requests.
   *
   * <p>By default, the queue factory will create <a
   * href='https://github.com/JCTools/JCTools/blob/master/jctools-core/src/main/java/org/jctools/queues/SpscArrayQueue.java'>{@code
   * SpscArrayQueue}</a> instances whose sizes are 4 times the {@link
   * ContinuousPagingOptions#getPageSize() page size}. Note that this might not be ideal if the page
   * size is expressed in {@link com.datastax.driver.core.ContinuousPagingOptions.PageUnit#BYTES
   * BYTES} rather than {@link com.datastax.driver.core.ContinuousPagingOptions.PageUnit#ROWS ROWS}.
   *
   * @param queueFactory the {@link QueueFactory} to use; cannot be {@code null}.
   * @return this builder (for method chaining).
   */
  @Override
  public AbstractBulkExecutorBuilder<ContinuousReactorBulkExecutor> withQueueFactory(
      QueueFactory<ReadResult> queueFactory) {
    return super.withQueueFactory(queueFactory);
  }

  @Override
  public ContinuousReactorBulkExecutor build() {
    return new ContinuousReactorBulkExecutor(this);
  }
}
