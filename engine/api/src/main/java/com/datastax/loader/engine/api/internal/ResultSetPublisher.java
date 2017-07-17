/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.api.internal;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.loader.engine.api.listener.ExecutionListener;
import com.datastax.loader.engine.api.result.Result;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import org.reactivestreams.Subscriber;

public abstract class ResultSetPublisher<T extends Result> extends ResultPublisher<T, ResultSet> {

  protected final int fetchThreshold;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  protected ResultSetPublisher(
      Statement statement,
      Session session,
      Executor executor,
      Optional<ExecutionListener> listener,
      Optional<RateLimiter> rateLimiter,
      Optional<Semaphore> requestPermits,
      boolean failFast) {
    super(statement, session, executor, listener, rateLimiter, requestPermits, failFast);
    int fetchSize =
        statement.getFetchSize() <= 0
            ? session.getCluster().getConfiguration().getQueryOptions().getFetchSize()
            : statement.getFetchSize();
    fetchThreshold = Math.max(1, fetchSize / 4);
  }

  @Override
  public void subscribe(Subscriber<? super T> subscriber) {
    super.subscribe(subscriber);
    fetchNextPage(subscriber, () -> session.executeAsync(statement));
  }
}
