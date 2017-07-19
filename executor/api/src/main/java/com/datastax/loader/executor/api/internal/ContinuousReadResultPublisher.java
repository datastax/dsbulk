/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.internal;

import com.datastax.driver.core.AsyncContinuousPagingResult;
import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.loader.executor.api.exception.BulkExecutionException;
import com.datastax.loader.executor.api.listener.ExecutionListener;
import com.datastax.loader.executor.api.result.ReadResult;
import com.datastax.loader.executor.api.internal.result.DefaultReadResult;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import org.reactivestreams.Subscriber;

public class ContinuousReadResultPublisher
    extends ResultPublisher<ReadResult, AsyncContinuousPagingResult> {

  private final ContinuousPagingSession session;
  private final ContinuousPagingOptions options;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public ContinuousReadResultPublisher(
      Statement statement,
      ContinuousPagingSession session,
      ContinuousPagingOptions options,
      Executor executor,
      Optional<ExecutionListener> listener,
      Optional<RateLimiter> rateLimiter,
      Optional<Semaphore> requestPermits,
      boolean failFast) {
    super(statement, session, executor, listener, rateLimiter, requestPermits, failFast);
    this.session = session;
    this.options = options;
  }

  @Override
  public void subscribe(Subscriber<? super ReadResult> subscriber) {
    super.subscribe(subscriber);
    fetchNextPage(subscriber, () -> session.executeContinuouslyAsync(statement, options));
  }

  @Override
  protected void consumePage(
      Subscriber<? super ReadResult> subscriber, AsyncContinuousPagingResult pagingResult) {
    for (Row row : pagingResult.currentPage()) {
      if (canceled) {
        pagingResult.cancel();
        return;
      }
      DefaultReadResult result = new DefaultReadResult(statement, row);
      onNext(subscriber, result);
    }
    if (pagingResult.isLast()) {
      onComplete(subscriber);
    } else if (!canceled) {
      fetchNextPage(subscriber, pagingResult::nextPage);
    }
  }

  @Override
  protected ReadResult toErrorResult(BulkExecutionException error) {
    return new DefaultReadResult(error);
  }
}
