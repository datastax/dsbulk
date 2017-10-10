/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.util.QueueDrainHelper;
import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Queue;

/** A builder for {@link DefaultRxJavaBulkExecutor} instances. */
public class DefaultRxJavaBulkExecutorBuilder
    extends AbstractBulkExecutorBuilder<DefaultRxJavaBulkExecutor> {

  DefaultRxJavaBulkExecutorBuilder(Session session) {
    super(session);
    queueFactory = statement -> createQueue(statement.getFetchSize() * 4);
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
  public AbstractBulkExecutorBuilder<DefaultRxJavaBulkExecutor> withQueueFactory(
      QueueFactory<ReadResult> queueFactory) {
    return super.withQueueFactory(queueFactory);
  }

  @Override
  public DefaultRxJavaBulkExecutor build() {
    return new DefaultRxJavaBulkExecutor(
        session,
        failFast,
        maxInFlightRequests,
        maxRequestsPerSecond,
        listener,
        executor.get(),
        queueFactory);
  }

  static Queue<ReadResult> createQueue(int capacityHint) {
    SimplePlainQueue<ReadResult> simpleQueue =
        (SimplePlainQueue<ReadResult>) QueueDrainHelper.<ReadResult>createQueue(capacityHint);
    return new AbstractQueue<ReadResult>() {

      @Override
      public boolean offer(ReadResult e) {
        return simpleQueue.offer(e);
      }

      @Override
      public ReadResult poll() {
        return simpleQueue.poll();
      }

      @Override
      public boolean isEmpty() {
        return simpleQueue.isEmpty();
      }

      @Override
      public void clear() {
        simpleQueue.clear();
      }

      @Override
      public ReadResult peek() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int size() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Iterator<ReadResult> iterator() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
