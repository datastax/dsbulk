/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.internal.emitter;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.listener.DefaultExecutionContext;
import com.datastax.dsbulk.executor.api.listener.ExecutionContext;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.Result;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public abstract class ResultEmitter<T extends Result, R> {

  final Statement statement;
  final Session session;

  private final Optional<Semaphore> requestPermits;
  private final Optional<ExecutionListener> listener;
  private final Optional<RateLimiter> rateLimiter;
  private final boolean failFast;
  private final Executor executor;
  private final int batchSize;
  private final ExecutionContext context = new DefaultExecutionContext();

  ResultEmitter(
      Statement statement,
      Session session,
      Executor executor,
      Optional<ExecutionListener> listener,
      Optional<RateLimiter> rateLimiter,
      Optional<Semaphore> requestPermits,
      boolean failFast) {
    this.statement = statement;
    this.session = session;
    this.executor = executor;
    this.listener = listener;
    this.rateLimiter = rateLimiter;
    this.requestPermits = requestPermits;
    this.failFast = failFast;
    if (statement instanceof BatchStatement) {
      batchSize = ((BatchStatement) statement).size();
    } else {
      batchSize = 1;
    }
  }

  public void start() {
    listener.ifPresent(l -> l.onExecutionStarted(statement, context));
  }

  void fetchNextPage(Callable<ListenableFuture<R>> query) {
    rateLimiter.ifPresent(limiter -> limiter.acquire(batchSize));
    requestPermits.ifPresent(permits -> permits.acquireUninterruptibly(batchSize));
    ListenableFuture<R> page;
    try {
      page = query.call();
    } catch (Throwable ex) {
      // in rare cases, the driver throws instead of failing the future
      requestPermits.ifPresent(permits -> permits.release(batchSize));
      onError(ex);
      return;
    }
    Futures.addCallback(
        page,
        new FutureCallback<R>() {
          @Override
          public void onSuccess(R rs) {
            requestPermits.ifPresent(permits -> permits.release(batchSize));
            consumePage(rs);
          }

          @Override
          public void onFailure(Throwable t) {
            requestPermits.ifPresent(permits -> permits.release(batchSize));
            onError(t);
          }
        },
        executor);
  }

  void onNext(T result) {
    listener.ifPresent(l -> l.onResultReceived(result, context));
    notifyOnNext(result);
  }

  void onComplete() {
    listener.ifPresent(l -> l.onExecutionCompleted(statement, context));
    notifyOnComplete();
  }

  private void onError(Throwable t) {
    BulkExecutionException error = new BulkExecutionException(t, statement);
    if (failFast) {
      listener.ifPresent(l -> l.onExecutionFailed(error, context));
      notifyOnError(error);
    } else {
      onNext(toErrorResult(error));
      listener.ifPresent(l -> l.onExecutionFailed(error, context));
      notifyOnComplete();
    }
  }

  protected abstract void notifyOnNext(T result);

  protected abstract void notifyOnComplete();

  protected abstract void notifyOnError(BulkExecutionException error);

  protected abstract boolean isCancelled();

  abstract void consumePage(R result);

  abstract T toErrorResult(BulkExecutionException error);
}
