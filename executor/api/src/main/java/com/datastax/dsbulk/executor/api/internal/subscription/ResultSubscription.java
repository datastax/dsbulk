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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public abstract class ResultSubscription<T extends Result, R>
    implements Subscription, FutureCallback<R> {

  protected final Statement statement;
  protected final Session session;
  protected final Executor executor;

  private final Subscriber<? super T> subscriber;
  private final Queue<T> queue;

  private final Optional<Semaphore> requestPermits;
  private final Optional<ExecutionListener> listener;
  private final Optional<RateLimiter> rateLimiter;

  private final boolean failFast;
  private final int size;
  private final ExecutionContext context = new DefaultExecutionContext();

  private volatile BulkExecutionException error;
  private volatile boolean done;
  private volatile boolean cancelled = false;

  private final Lock lock = new ReentrantLock();
  private final Condition notFull = lock.newCondition();

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
    listener.ifPresent(l -> l.onExecutionStarted(statement, context));
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
      notifyNotFull();
    }
  }

  boolean isCancelled() {
    return cancelled;
  }

  long getRequested() {
    return requested;
  }

  ListenableFuture<R> fetchNextPage(Callable<ListenableFuture<R>> query) {
    rateLimiter.ifPresent(limiter -> limiter.acquire(size));
    requestPermits.ifPresent(permits -> permits.acquireUninterruptibly(size));
    try {
      return query.call();
    } catch (Throwable ex) {
      // in rare cases, the driver throws instead of failing the future
      requestPermits.ifPresent(permits -> permits.release(size));
      onError(ex);
      return null;
    }
  }

  @Override
  public void onSuccess(R rs) {
    requestPermits.ifPresent(permits -> permits.release(size));
    consumePage(rs);
  }

  @Override
  public void onFailure(Throwable t) {
    requestPermits.ifPresent(permits -> permits.release(size));
    onError(t);
  }

  void onNext(T result) {
    listener.ifPresent(l -> l.onResultReceived(result, context));
    while (!queue.offer(result)) {
      drain();
      lock.lock();
      try {
        notFull.await(10, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        lock.unlock();
      }
      if (isCancelled()) {
        return;
      }
    }
    drain();
  }

  void onComplete() {
    listener.ifPresent(l -> l.onExecutionCompleted(statement, context));
    done = true;
    drain();
  }

  void onError(Throwable t) {
    BulkExecutionException error = new BulkExecutionException(t, statement);
    if (failFast) {
      this.error = error;
    } else {
      onNext(toErrorResult(error));
    }
    listener.ifPresent(l -> l.onExecutionFailed(error, context));
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
          notifyNotFull();
          return;
        }

        boolean d = done;

        T result = queue.poll();
        notifyNotFull();

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
          notifyNotFull();
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
        Operators.produced(REQUESTED, this, e);
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

  private void notifyNotFull() {
    lock.lock();
    try {
      notFull.signal();
    } finally {
      lock.unlock();
    }
  }

  protected abstract void consumePage(R result);

  protected abstract T toErrorResult(BulkExecutionException error);
}
