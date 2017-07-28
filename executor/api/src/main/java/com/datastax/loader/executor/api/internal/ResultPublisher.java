/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.internal;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.loader.executor.api.exception.BulkExecutionException;
import com.datastax.loader.executor.api.internal.listener.DefaultExecutionContext;
import com.datastax.loader.executor.api.listener.ExecutionContext;
import com.datastax.loader.executor.api.listener.ExecutionListener;
import com.datastax.loader.executor.api.result.Result;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public abstract class ResultPublisher<T extends Result, R> implements Publisher<T>, Subscription {

  protected final Statement statement;
  protected final ExecutionContext context = new DefaultExecutionContext();
  protected final RequestCounter requestCounter = new RequestCounter();
  protected final Optional<ExecutionListener> listener;
  protected final Optional<RateLimiter> rateLimiter;
  protected final Session session;
  protected final Optional<Semaphore> requestPermits;
  protected final boolean failFast;
  protected final Executor executor;
  protected volatile boolean canceled = false;

  protected ResultPublisher(
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
  }

  @Override
  public void request(long n) {
    requestCounter.signalRequested(n);
  }

  @Override
  public void cancel() {
    canceled = true;
  }

  @Override
  public void subscribe(Subscriber<? super T> subscriber) {
    subscriber.onSubscribe(this);
    listener.ifPresent(l -> l.onExecutionStarted(statement, context));
  }

  protected void fetchNextPage(
      Subscriber<? super T> subscriber, Callable<ListenableFuture<R>> query) {
    rateLimiter.ifPresent(RateLimiter::acquire);
    requestPermits.ifPresent(Semaphore::acquireUninterruptibly);
    ListenableFuture<R> page;
    try {
      page = query.call();
    } catch (Throwable ex) {
      // in rare cases, the driver throws instead of failing the future
      requestPermits.ifPresent(Semaphore::release);
      onError(subscriber, ex);
      return;
    }
    Futures.addCallback(
        page,
        new FutureCallback<R>() {
          @Override
          public void onSuccess(R rs) {
            requestPermits.ifPresent(Semaphore::release);
            consumePage(subscriber, rs);
          }

          @Override
          public void onFailure(Throwable t) {
            requestPermits.ifPresent(Semaphore::release);
            onError(subscriber, t);
          }
        },
        executor);
  }

  protected void onNext(Subscriber<? super T> subscriber, T result) {
    if (canceled) return;
    requestCounter.awaitRequested();
    listener.ifPresent(l -> l.onResultReceived(result, context));
    subscriber.onNext(result);
  }

  protected void onComplete(Subscriber<? super T> subscriber) {
    listener.ifPresent(l -> l.onExecutionCompleted(statement, context));
    subscriber.onComplete();
  }

  protected void onError(Subscriber<? super T> subscriber, Throwable t) {
    BulkExecutionException error = new BulkExecutionException(t, statement);
    if (failFast) {
      listener.ifPresent(l -> l.onExecutionFailed(error, context));
      subscriber.onError(error);
    } else {
      onNext(subscriber, toErrorResult(error));
      listener.ifPresent(l -> l.onExecutionFailed(error, context));
      subscriber.onComplete();
    }
  }

  protected abstract void consumePage(Subscriber<? super T> subscriber, R result);

  protected abstract T toErrorResult(BulkExecutionException error);
}
