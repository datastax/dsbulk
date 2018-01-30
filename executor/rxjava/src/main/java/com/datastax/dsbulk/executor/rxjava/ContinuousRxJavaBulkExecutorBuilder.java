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
import com.datastax.dsbulk.executor.api.QueueFactory;
import com.datastax.dsbulk.executor.api.result.ReadResult;

/** A builder for {@link ContinuousRxJavaBulkExecutor} instances. */
public class ContinuousRxJavaBulkExecutorBuilder
    extends AbstractBulkExecutorBuilder<ContinuousRxJavaBulkExecutor> {

  final ContinuousPagingSession continuousPagingSession;
  ContinuousPagingOptions options = ContinuousPagingOptions.builder().build();

  ContinuousRxJavaBulkExecutorBuilder(ContinuousPagingSession continuousPagingSession) {
    super(continuousPagingSession);
    this.continuousPagingSession = continuousPagingSession;
    queueFactory =
        statement -> DefaultRxJavaBulkExecutorBuilder.createQueue(options.getPageSize() * 4);
  }

  public AbstractBulkExecutorBuilder<ContinuousRxJavaBulkExecutor> withContinuousPagingOptions(
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
  public AbstractBulkExecutorBuilder<ContinuousRxJavaBulkExecutor> withQueueFactory(
      QueueFactory<ReadResult> queueFactory) {
    return super.withQueueFactory(queueFactory);
  }

  @Override
  public ContinuousRxJavaBulkExecutor build() {
    return new ContinuousRxJavaBulkExecutor(this);
  }
}
