/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.internal.subscription;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.result.DefaultWriteResult;
import com.datastax.dsbulk.executor.api.listener.ExecutionContext;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Subscriber;

public class WriteResultSubscription extends ResultSubscription<WriteResult, ResultSet> {

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public WriteResultSubscription(
      Subscriber<? super WriteResult> subscriber,
      Statement statement,
      Session session,
      Executor executor,
      Optional<ExecutionListener> listener,
      Optional<RateLimiter> rateLimiter,
      Optional<Semaphore> requestPermits,
      boolean failFast) {
    super(
        subscriber,
        new SingleElementQueue<>(),
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
    listener.ifPresent(l -> l.onWriteRequestStarted(statement, local));
  }

  @Override
  void onRequestSuccessful(ResultSet rs, ExecutionContext local) {
    assert rs.isFullyFetched();
    listener.ifPresent(l -> l.onWriteRequestSuccessful(statement, local));
    onNext(new DefaultWriteResult(statement, rs.getExecutionInfo()));
    onComplete();
  }

  @Override
  void onRequestFailed(Throwable t, ExecutionContext local) {
    listener.ifPresent(l -> l.onWriteRequestFailed(statement, t, local));
    onError(t);
  }

  @Override
  WriteResult toErrorResult(BulkExecutionException error) {
    return new DefaultWriteResult(error);
  }

  private static final class SingleElementQueue<E> extends AtomicReference<E> implements Queue<E> {

    @Override
    public boolean add(E e) {
      if (!offer(e)) {
        throw new IllegalStateException("Queue is full");
      }
      return true;
    }

    @Override
    public void clear() {
      set(null);
    }

    @Override
    public boolean contains(Object o) {
      return Objects.equals(get(), o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      return false;
    }

    @Override
    public E element() {
      return get();
    }

    @Override
    public boolean isEmpty() {
      return get() == null;
    }

    @Override
    public boolean offer(E e) {
      if (get() != null) {
        return false;
      }
      lazySet(e);
      return true;
    }

    @Override
    public E peek() {
      return get();
    }

    @Override
    public E poll() {
      E v = get();
      if (v != null) {
        lazySet(null);
      }
      return v;
    }

    @Override
    public E remove() {
      return getAndSet(null);
    }

    @Override
    public int size() {
      return get() == null ? 0 : 1;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<E> iterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
      throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T1> T1[] toArray(T1[] a) {
      throw new UnsupportedOperationException();
    }
  }
}
