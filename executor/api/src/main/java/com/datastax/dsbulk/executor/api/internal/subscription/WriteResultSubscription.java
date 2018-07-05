/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.internal.subscription;

import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.result.DefaultWriteResult;
import com.datastax.dsbulk.executor.api.listener.ExecutionContext;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import org.reactivestreams.Subscriber;

public class WriteResultSubscription extends ResultSubscription<WriteResult, AsyncResultSet> {

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public WriteResultSubscription(
      Subscriber<? super WriteResult> subscriber,
      Statement statement,
      Optional<ExecutionListener> listener,
      Optional<Semaphore> requestPermits,
      Optional<RateLimiter> rateLimiter,
      boolean failFast) {
    super(subscriber, statement, listener, requestPermits, rateLimiter, failFast);
  }

  @Override
  Page toPage(AsyncResultSet rs, ExecutionContext local) {
    if (rs.hasMorePages()) {
      return toErrorPage(
          new IllegalStateException(
              "Got a non-empty result set, is this really a write statement?"));
    }
    Iterator<WriteResult> iterator =
        Collections.<WriteResult>singleton(new DefaultWriteResult(statement, rs.getExecutionInfo()))
            .iterator();
    return new Page(iterator, null);
  }

  @Override
  WriteResult toErrorResult(BulkExecutionException error) {
    return new DefaultWriteResult(error);
  }

  @Override
  void onRequestStarted(ExecutionContext local) {
    listener.ifPresent(l -> l.onWriteRequestStarted(statement, local));
  }

  @Override
  void onRequestSuccessful(AsyncResultSet rs, ExecutionContext local) {
    listener.ifPresent(l -> l.onWriteRequestSuccessful(statement, local));
  }

  @Override
  void onRequestFailed(Throwable t, ExecutionContext local) {
    listener.ifPresent(l -> l.onWriteRequestFailed(statement, t, local));
  }
}
