/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.internal.subscription;

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
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public abstract class ResultSubscription<T extends Result, R> implements Subscription {

  final Statement statement;
  final Session session;
  final Optional<ExecutionListener> listener;

  private final Subscriber<? super T> subscriber;
  private final Queue<T> queue;
  private final Executor executor;
  private final Optional<Semaphore> requestPermits;
  private final Optional<RateLimiter> rateLimiter;
  private final boolean failFast;
  private final int size;

  private final DefaultExecutionContext global = new DefaultExecutionContext();

  private volatile BulkExecutionException error;
  private volatile boolean done;
  private volatile boolean cancelled = false;

  @SuppressWarnings("unused")
  private volatile int wip;

  private static final AtomicIntegerFieldUpdater<ResultSubscription> WIP =
      AtomicIntegerFieldUpdater.newUpdater(ResultSubscription.class, "wip");

  @SuppressWarnings("unused")
  private volatile long requested;

  private static final AtomicLongFieldUpdater<ResultSubscription> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(ResultSubscription.class, "requested");

  ResultSubscription(
      Subscriber<? super T> subscriber,
      Queue<T> queue,
      Statement statement,
      Session session,
      Executor executor,
      Optional<ExecutionListener> listener,
      Optional<RateLimiter> rateLimiter,
      Optional<Semaphore> requestPermits,
      boolean failFast) {
    this.subscriber = subscriber;
    this.queue = queue;
    this.statement = statement;
    this.session = session;
    this.executor = executor;
    this.listener = listener;
    this.rateLimiter = rateLimiter;
    this.requestPermits = requestPermits;
    this.failFast = failFast;
    if (statement instanceof BatchStatement) {
      size = ((BatchStatement) statement).size();
    } else {
      size = 1;
    }
  }

  public void start() {
    global.start();
    listener.ifPresent(l -> l.onExecutionStarted(statement, global));
  }

  @Override
  public void request(long n) {
    if (Operators.validate(n)) {
      Operators.addCap(REQUESTED, this, n);
      drain();
    }
  }

  @Override
  public void cancel() {
    cancelled = true;
    if (WIP.getAndIncrement(this) == 0) {
      queue.clear();
    }
  }

  boolean isCancelled() {
    return cancelled;
  }

  void fetchNextPage(Supplier<ListenableFuture<R>> request) {
    DefaultExecutionContext local = new DefaultExecutionContext();
    rateLimiter.ifPresent(limiter -> limiter.acquire(size));
    requestPermits.ifPresent(permits -> permits.acquireUninterruptibly(1));
    local.start();
    onRequestStarted(local);
    ListenableFuture<R> page;
    try {
      page = request.get();
    } catch (Exception e) {
      requestPermits.ifPresent(permits -> permits.release(1));
      local.stop();
      onRequestFailed(e, local);
      return;
    }
    Futures.addCallback(
        page,
        new FutureCallback<R>() {
          @Override
          public void onSuccess(R result) {
            local.stop();
            requestPermits.ifPresent(permits -> permits.release(1));
            onRequestSuccessful(result, local);
          }

          @Override
          public void onFailure(Throwable t) {
            local.stop();
            requestPermits.ifPresent(permits -> permits.release(1));
            onRequestFailed(t, local);
          }
        },
        executor);
  }

  void onNext(T result) {
    while (!queue.offer(result)) {
      drain();
      LockSupport.parkNanos(10);
      if (isCancelled()) {
        return;
      }
    }
    drain();
  }

  void onComplete() {
    global.stop();
    listener.ifPresent(l -> l.onExecutionSuccessful(statement, global));
    done = true;
    drain();
  }

  void onError(Throwable t) {
    global.stop();
    BulkExecutionException error = new BulkExecutionException(t, statement);
    if (failFast) {
      this.error = error;
    } else {
      onNext(toErrorResult(error));
    }
    listener.ifPresent(l -> l.onExecutionFailed(error, global));
    done = true;
    drain();
  }

  private void drain() {
    if (WIP.getAndIncrement(this) != 0) {
      return;
    }
    int missed = 1;
    for (; ; ) {
      long r = requested;
      long e = 0L;

      while (e != r) {
        if (isCancelled()) {
          queue.clear();
          return;
        }

        boolean d = done;

        T result = queue.poll();

        boolean empty = result == null;

        if (d && empty) {
          stop();
          return;
        }

        if (empty) {
          break;
        }

        subscriber.onNext(result);

        e++;
      }

      if (e == r) {
        if (isCancelled()) {
          queue.clear();
          return;
        }

        boolean d = done;

        boolean empty = queue.isEmpty();

        if (d && empty) {
          stop();
          return;
        }
      }

      if (e != 0) {
        Operators.subCap(REQUESTED, this, e);
      }

      missed = WIP.addAndGet(this, -missed);
      if (missed == 0) {
        break;
      }
    }
  }

  private void stop() {
    if (!isCancelled()) {
      BulkExecutionException ex = error;
      if (ex != null) {
        subscriber.onError(ex);
      } else {
        subscriber.onComplete();
      }
    }
  }

  abstract void onRequestStarted(ExecutionContext local);

  abstract void onRequestSuccessful(R result, ExecutionContext local);

  abstract void onRequestFailed(Throwable t, ExecutionContext local);

  abstract T toErrorResult(BulkExecutionException error);
}
