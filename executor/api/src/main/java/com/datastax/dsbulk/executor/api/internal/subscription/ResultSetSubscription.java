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
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.Result;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import org.reactivestreams.Subscriber;

public abstract class ResultSetSubscription<T extends Result>
    extends ResultSubscription<T, ResultSet> {

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  ResultSetSubscription(
      Subscriber<? super T> subscriber,
      Queue<T> queue,
      Statement statement,
      Session session,
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
  }

  @Override
  public void start() {
    super.start();
    ListenableFuture<ResultSet> firstPage = fetchNextPage(() -> session.executeAsync(statement));
    if (firstPage != null) {
      addCallback(firstPage);
    }
  }
}
