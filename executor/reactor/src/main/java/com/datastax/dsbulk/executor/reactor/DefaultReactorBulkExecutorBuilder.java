/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.reactor;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.dsbulk.executor.api.QueueFactory;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import reactor.util.concurrent.Queues;

/** A builder for {@link DefaultReactorBulkExecutor} instances. */
public class DefaultReactorBulkExecutorBuilder
    extends AbstractBulkExecutorBuilder<DefaultReactorBulkExecutor> {

  DefaultReactorBulkExecutorBuilder(Session session) {
    super(session);
    queueFactory = statement -> Queues.<ReadResult>get(statement.getFetchSize() * 4).get();
  }

  /**
   * Sets the {@link QueueFactory} to use when executing read requests.
   *
   * <p>By default, the queue factory will create <a
   * href='https://github.com/JCTools/JCTools/blob/master/jctools-core/src/main/java/org/jctools/queues/SpscArrayQueue.java'>{@code
   * SpscArrayQueue}</a> instances whose sizes are 4 times the statement's {@link
   * Statement#getFetchSize() fetch size}.
   *
   * @param queueFactory the {@link QueueFactory} to use; cannot be {@code null}.
   * @return this builder (for method chaining).
   */
  @Override
  public AbstractBulkExecutorBuilder<DefaultReactorBulkExecutor> withQueueFactory(
      QueueFactory<ReadResult> queueFactory) {
    return super.withQueueFactory(queueFactory);
  }

  @Override
  public DefaultReactorBulkExecutor build() {
    return new DefaultReactorBulkExecutor(this);
  }
}
