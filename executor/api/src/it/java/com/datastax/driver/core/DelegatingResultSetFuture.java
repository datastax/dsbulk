/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.driver.core;

import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DelegatingResultSetFuture implements ResultSetFuture {

  private final SettableFuture<ResultSet> delegate;

  public DelegatingResultSetFuture(SettableFuture<ResultSet> delegate) {
    this.delegate = delegate;
  }

  @Override
  public ResultSet get() throws InterruptedException, ExecutionException {
    return delegate.get();
  }

  @Override
  public ResultSet get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return delegate.get(timeout, unit);
  }

  @Override
  public ResultSet getUninterruptibly() {
    try {
      return Uninterruptibles.getUninterruptibly(delegate);
    } catch (ExecutionException e) {
      throw DriverThrowables.propagateCause(e);
    }
  }

  @Override
  public ResultSet getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
    try {
      return Uninterruptibles.getUninterruptibly(this, timeout, unit);
    } catch (ExecutionException e) {
      throw DriverThrowables.propagateCause(e);
    }
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return delegate.cancel(mayInterruptIfRunning);
  }

  @Override
  public void addListener(Runnable listener, Executor executor) {
    delegate.addListener(listener, executor);
  }

  @Override
  public boolean isCancelled() {
    return delegate.isCancelled();
  }

  @Override
  public boolean isDone() {
    return delegate.isDone();
  }
}
