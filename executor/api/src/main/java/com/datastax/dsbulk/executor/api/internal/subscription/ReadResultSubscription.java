/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.internal.subscription;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.result.DefaultReadResult;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import org.reactivestreams.Subscriber;

public class ReadResultSubscription extends ResultSetSubscription<ReadResult> {

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public ReadResultSubscription(
      Subscriber<? super ReadResult> subscriber,
      Queue<ReadResult> queue,
      Statement statement,
      Session session,
      Executor executor,
      Optional<ExecutionListener> listener,
      Optional<RateLimiter> rateLimiter,
      Optional<Semaphore> requestPermits,
      boolean failFast) {
    super(
        subscriber,
        queue,
        statement,
        session,
        executor,
        listener,
        rateLimiter,
        requestPermits,
        failFast);
  }

  @Override
  protected void consumePage(ResultSet rs) {
    for (Row row : rs) {
      DefaultReadResult result = new DefaultReadResult(statement, rs.getExecutionInfo(), row);
      if (isCancelled()) {
        break;
      }
      onNext(result);
    }
    boolean lastPage = rs.getExecutionInfo().getPagingState() == null;
    if (lastPage) {
      onComplete();
    } else if (!isCancelled()) {
      fetchNextPage(rs::fetchMoreResults);
    }
  }

  @Override
  protected ReadResult toErrorResult(BulkExecutionException error) {
    return new DefaultReadResult(error);
  }
}
