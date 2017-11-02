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
import com.datastax.dsbulk.executor.api.listener.ExecutionContext;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.ReadResult;
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

  @Override
  public void start() {
    super.start();
    fetchNextPage(() -> session.executeContinuouslyAsync(statement, options));
  }

  @Override
  void onRequestStarted(ExecutionContext local) {
    listener.ifPresent(l -> l.onReadRequestStarted(statement, local));
  }

  @Override
  void onRequestSuccessful(AsyncContinuousPagingResult page, ExecutionContext local) {
    listener.ifPresent(l -> l.onReadRequestSuccessful(statement, local));
    for (Row row : page.currentPage()) {
      if (isCancelled()) {
        page.cancel();
        return;
      }
      listener.ifPresent(l -> l.onRowReceived(row, local));
      onNext(new DefaultReadResult(statement, page.getExecutionInfo(), row));
    }
    if (page.isLast()) {
      onComplete();
    } else if (!isCancelled()) {
      fetchNextPage(page::nextPage);
    }
  }

  @Override
  void onRequestFailed(Throwable t, ExecutionContext local) {
    listener.ifPresent(l -> l.onReadRequestFailed(statement, t, local));
    onError(t);
  }

  @Override
  protected ReadResult toErrorResult(BulkExecutionException error) {
    return new DefaultReadResult(error);
  }
}
