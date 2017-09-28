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
import com.datastax.dsbulk.executor.api.listener.ExecutionContext;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import org.reactivestreams.Subscriber;

public class ReadResultSubscription extends ResultSubscription<ReadResult, ResultSet> {

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
  public void start() {
    super.start();
    fetchNextPage(() -> session.executeAsync(statement));
  }

  @Override
  void onRequestStarted(ExecutionContext local) {
    listener.ifPresent(l -> l.onReadRequestStarted(statement, local));
  }

  @Override
  void onRequestSuccessful(ResultSet page, ExecutionContext local) {
    listener.ifPresent(
        l -> l.onReadRequestSuccessful(statement, page.getAvailableWithoutFetching(), local));
    for (Row row : page) {
      if (isCancelled()) {
        return;
      }
      onNext(new DefaultReadResult(statement, page.getExecutionInfo(), row));
    }
    boolean lastPage = page.getExecutionInfo().getPagingState() == null;
    if (lastPage) {
      onComplete();
    } else if (!isCancelled()) {
      fetchNextPage(page::fetchMoreResults);
    }
  }

  @Override
  void onRequestFailed(Throwable t, ExecutionContext local) {
    listener.ifPresent(l -> l.onReadRequestFailed(statement, t, local));
    onError(t);
  }

  @Override
  ReadResult toErrorResult(BulkExecutionException error) {
    return new DefaultReadResult(error);
  }
}
