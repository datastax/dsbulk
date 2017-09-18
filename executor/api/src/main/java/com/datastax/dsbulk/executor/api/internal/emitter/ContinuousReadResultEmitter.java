/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.internal.emitter;

import com.datastax.driver.core.AsyncContinuousPagingResult;
import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.result.DefaultReadResult;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

public abstract class ContinuousReadResultEmitter
    extends ResultEmitter<ReadResult, AsyncContinuousPagingResult> {

  private final ContinuousPagingSession session;
  private final ContinuousPagingOptions options;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public ContinuousReadResultEmitter(
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
  public void start() {
    super.start();
    fetchNextPage(() -> session.executeContinuouslyAsync(statement, options));
  }

  @Override
  protected void consumePage(AsyncContinuousPagingResult pagingResult) {
    for (Row row : pagingResult.currentPage()) {
      if (isCancelled()) {
        pagingResult.cancel();
        return;
      }
      DefaultReadResult result =
          new DefaultReadResult(statement, pagingResult.getExecutionInfo(), row);
      onNext(result);
    }
    if (pagingResult.isLast()) {
      onComplete();
    } else if (!isCancelled()) {
      fetchNextPage(pagingResult::nextPage);
    }
  }

  @Override
  protected ReadResult toErrorResult(BulkExecutionException error) {
    return new DefaultReadResult(error);
  }
}
