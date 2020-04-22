/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.commons.concurrent;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

/**
 * A scalable {@link ThreadPoolExecutor} that favors spawning new threads over enqueuing tasks.
 *
 * @see <a href="https://dzone.com/articles/scalable-java-thread-pool-executor">A Scalable Java
 *     Thread Pool Executor</a>
 */
public class ScalableThreadPoolExecutor extends ThreadPoolExecutor {

  /**
   * Creates a new scalable thread pool.
   *
   * @param corePoolSize the core pool size.
   * @param maximumPoolSize the maximum pool size.
   * @param keepAliveTime the keep-alive time.
   * @param keepAliveUnit the keep-alive unit.
   */
  public ScalableThreadPoolExecutor(
      int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit keepAliveUnit) {
    super(
        corePoolSize,
        maximumPoolSize,
        keepAliveTime,
        keepAliveUnit,
        new InterceptingQueue<>(),
        // Add rejected work to the queue
        (r, e) -> e.getQueue().add(r));
  }

  @Override
  public void setRejectedExecutionHandler(RejectedExecutionHandler handler) {
    throw new UnsupportedOperationException("Cannot set rejected execution handler");
  }

  private static class InterceptingQueue<E> implements TransferQueue<E> {

    private final TransferQueue<E> delegate = new LinkedTransferQueue<>();

    @Override
    public boolean offer(@NonNull E o) {
      // intercept calls to offer() and try to transfer the work directly to an available thread.
      // If this fails, the executor will spawn a new thread if possible. If a new thread could not
      // be created, the rejected execution handler above will be invoked and the element will be
      // enqueued for later execution.
      return tryTransfer(o);
    }

    @Override
    public boolean offer(E e, long timeout, @NonNull TimeUnit unit) throws InterruptedException {
      return tryTransfer(e, timeout, unit);
    }

    @Override
    public boolean add(@NonNull E o) {
      return this.delegate.add(o);
    }

    @Override
    public void put(@NonNull E e) throws InterruptedException {
      this.delegate.put(e);
    }

    @Override
    public E poll() {
      return this.delegate.poll();
    }

    @Override
    public E poll(long timeout, @NonNull TimeUnit unit) throws InterruptedException {
      return this.delegate.poll(timeout, unit);
    }

    @NonNull
    @Override
    public E take() throws InterruptedException {
      return this.delegate.take();
    }

    @Override
    public E remove() {
      return this.delegate.remove();
    }

    @Override
    public boolean remove(Object o) {
      return this.delegate.remove(o);
    }

    @Override
    public E peek() {
      return this.delegate.peek();
    }

    @Override
    public E element() {
      return this.delegate.element();
    }

    @Override
    public int size() {
      return this.delegate.size();
    }

    @Override
    public boolean isEmpty() {
      return this.delegate.isEmpty();
    }

    @NonNull
    @Override
    public Iterator<E> iterator() {
      return this.delegate.iterator();
    }

    @NonNull
    @Override
    public Object[] toArray() {
      return this.delegate.toArray();
    }

    @NonNull
    @SuppressWarnings("SuspiciousToArrayCall")
    @Override
    public <T> T[] toArray(@NonNull T[] a) {
      return this.delegate.toArray(a);
    }

    @Override
    public boolean containsAll(@NonNull Collection<?> c) {
      return this.delegate.containsAll(c);
    }

    @Override
    public boolean addAll(@NonNull Collection<? extends E> c) {
      return this.delegate.addAll(c);
    }

    @Override
    public boolean removeAll(@NonNull Collection<?> c) {
      return this.delegate.removeAll(c);
    }

    @Override
    public boolean retainAll(@NonNull Collection<?> c) {
      return this.delegate.retainAll(c);
    }

    @Override
    public void clear() {
      this.delegate.clear();
    }

    @Override
    public int remainingCapacity() {
      return this.delegate.remainingCapacity();
    }

    @Override
    public boolean contains(Object o) {
      return this.delegate.contains(o);
    }

    @Override
    public int drainTo(@NonNull Collection<? super E> c) {
      return this.delegate.drainTo(c);
    }

    @Override
    public int drainTo(@NonNull Collection<? super E> c, int maxElements) {
      return this.delegate.drainTo(c, maxElements);
    }

    @Override
    public boolean tryTransfer(@NonNull E e) {
      return this.delegate.tryTransfer(e);
    }

    @Override
    public void transfer(@NonNull E e) throws InterruptedException {
      this.delegate.transfer(e);
    }

    @Override
    public boolean tryTransfer(@NonNull E e, long timeout, @NonNull TimeUnit unit)
        throws InterruptedException {
      return this.delegate.tryTransfer(e, timeout, unit);
    }

    @Override
    public boolean hasWaitingConsumer() {
      return this.delegate.hasWaitingConsumer();
    }

    @Override
    public int getWaitingConsumerCount() {
      return this.delegate.getWaitingConsumerCount();
    }
  }
}
