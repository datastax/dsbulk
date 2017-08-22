/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.internal;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.loader.executor.api.exception.BulkExecutionException;
import com.datastax.loader.executor.api.internal.result.DefaultWriteResult;
import com.datastax.loader.executor.api.listener.ExecutionListener;
import com.datastax.loader.executor.api.result.WriteResult;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import org.reactivestreams.Subscriber;

public class WriteResultPublisher extends ResultSetPublisher<WriteResult> {

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public WriteResultPublisher(
      Statement statement,
      Session session,
      Executor executor,
      Optional<ExecutionListener> listener,
      Optional<RateLimiter> rateLimiter,
      Optional<Semaphore> requestPermits,
      boolean failFast) {
    super(statement, session, executor, listener, rateLimiter, requestPermits, failFast);
  }

  @Override
  protected void consumePage(Subscriber<? super WriteResult> subscriber, ResultSet rs) {
    assert rs.isFullyFetched();
    onNext(subscriber, new DefaultWriteResult(statement, rs.getExecutionInfo()));
    onComplete(subscriber);
  }

  @Override
  protected WriteResult toErrorResult(BulkExecutionException error) {
    return new DefaultWriteResult(error);
  }
}
