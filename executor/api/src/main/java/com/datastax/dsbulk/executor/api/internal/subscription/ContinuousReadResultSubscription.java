/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.internal.subscription;

import com.datastax.driver.core.AsyncContinuousPagingResult;
import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.result.DefaultReadResult;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import org.reactivestreams.Subscriber;

public class ContinuousReadResultSubscription
    extends ResultSubscription<ReadResult, AsyncContinuousPagingResult> {

  private final ContinuousPagingSession session;
  private final ContinuousPagingOptions options;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public ContinuousReadResultSubscription(
      Subscriber<? super ReadResult> subscriber,
      Queue<ReadResult> queue,
      Statement statement,
      ContinuousPagingSession session,
      ContinuousPagingOptions options,
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
    this.session = session;
    this.options = options;
  }

  public void start() {
    super.start();
    ListenableFuture<AsyncContinuousPagingResult> firstPage =
        fetchNextPage(() -> session.executeContinuouslyAsync(statement, options));
    if (firstPage != null) {
      Futures.addCallback(firstPage, this, executor);
    }
  }

  @Override
  protected void consumePage(AsyncContinuousPagingResult pagingResult) {
    boolean lastPage = pagingResult.isLast();
    for (Row row : pagingResult.currentPage()) {
      if (isCancelled()) {
        pagingResult.cancel();
        return;
      }
      DefaultReadResult result =
          new DefaultReadResult(statement, pagingResult.getExecutionInfo(), row);
      onNext(result);
    }
    if (lastPage) {
      onComplete();
    } else {
      ListenableFuture<AsyncContinuousPagingResult> nextPage =
          fetchNextPage(pagingResult::nextPage);
      if (nextPage == null) {
        return;
      }
      Futures.addCallback(nextPage, this, executor);
    }
  }

  @Override
  protected ReadResult toErrorResult(BulkExecutionException error) {
    return new DefaultReadResult(error);
  }
}
