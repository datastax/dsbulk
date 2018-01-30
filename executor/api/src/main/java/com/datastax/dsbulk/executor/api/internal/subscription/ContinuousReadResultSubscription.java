/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class ContinuousReadResultSubscription
    extends ResultSubscription<ReadResult, AsyncContinuousPagingResult> {

  private final ContinuousPagingSession continuousPagingSession;
  private final ContinuousPagingOptions options;

  public ContinuousReadResultSubscription(
      Subscriber<? super ReadResult> subscriber,
      Queue<ReadResult> queue,
      Statement statement,
      ContinuousPagingSession continuousPagingSession,
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
        continuousPagingSession,
        executor,
        listener,
        rateLimiter,
        requestPermits,
        failFast);
    this.continuousPagingSession = continuousPagingSession;
    this.options = options;
  }

  @Override
  public void start() {
    super.start();
    fetchNextPage(() -> continuousPagingSession.executeContinuouslyAsync(statement, options));
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
