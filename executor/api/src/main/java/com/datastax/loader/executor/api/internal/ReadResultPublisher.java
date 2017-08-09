/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.internal;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.loader.executor.api.exception.BulkExecutionException;
import com.datastax.loader.executor.api.internal.result.DefaultReadResult;
import com.datastax.loader.executor.api.listener.ExecutionListener;
import com.datastax.loader.executor.api.result.ReadResult;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import org.reactivestreams.Subscriber;

public class ReadResultPublisher extends ResultSetPublisher<ReadResult> {

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public ReadResultPublisher(
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
  protected void consumePage(Subscriber<? super ReadResult> subscriber, ResultSet rs) {
    for (Row row : rs) {
      DefaultReadResult result = new DefaultReadResult(statement, row);
      if (canceled) {
        break;
      }
      onNext(subscriber, result);
    }
    boolean lastPage = rs.getExecutionInfo().getPagingState() == null;
    if (lastPage) {
      onComplete(subscriber);
    } else if (!canceled) {
      fetchNextPage(subscriber, rs::fetchMoreResults);
    }
  }

  @Override
  protected ReadResult toErrorResult(BulkExecutionException error) {
    return new DefaultReadResult(error);
  }
}
