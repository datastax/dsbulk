/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.internal.subscription;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.result.DefaultWriteResult;
import com.datastax.dsbulk.executor.api.listener.ExecutionContext;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Semaphore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Subscriber;

public class WriteResultSubscription extends ResultSubscription<WriteResult, ResultSet> {

  @SuppressWarnings("UnstableApiUsage")
  public WriteResultSubscription(
      @NotNull Subscriber<? super WriteResult> subscriber,
      @NotNull Statement statement,
      @Nullable ExecutionListener listener,
      @Nullable Semaphore maxConcurrentRequests,
      @Nullable Semaphore maxConcurrentQueries,
      @Nullable RateLimiter rateLimiter,
      boolean failFast) {
    super(
        subscriber,
        statement,
        listener,
        maxConcurrentRequests,
        maxConcurrentQueries,
        rateLimiter,
        failFast);
  }

  @Override
  Page toPage(ResultSet rs, ExecutionContext local) {
    Iterator<WriteResult> iterator =
        Collections.<WriteResult>singleton(new DefaultWriteResult(statement, rs)).iterator();
    return new Page(iterator, null);
  }

  @Override
  WriteResult toErrorResult(BulkExecutionException error) {
    return new DefaultWriteResult(error);
  }

  @Override
  void onBeforeRequestStarted() {
    if (rateLimiter != null) {
      rateLimiter.acquire(batchSize);
    }
    super.onBeforeRequestStarted();
  }

  @Override
  void onRequestStarted(ExecutionContext local) {
    if (listener != null) {
      listener.onWriteRequestStarted(statement, local);
    }
  }

  @Override
  void onRequestSuccessful(ResultSet rs, ExecutionContext local) {
    if (listener != null) {
      listener.onWriteRequestSuccessful(statement, local);
    }
  }

  @Override
  void onRequestFailed(Throwable t, ExecutionContext local) {
    if (listener != null) {
      listener.onWriteRequestFailed(statement, t, local);
    }
  }

  @Override
  boolean isLastPage(ResultSet page) {
    return true;
  }
}
