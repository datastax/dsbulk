/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.internal.emitter;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.result.DefaultWriteResult;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

public abstract class WriteResultEmitter extends ResultSetEmitter<WriteResult> {

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public WriteResultEmitter(
      Statement statement,
      Session session,
      Executor executor,
      Optional<ExecutionListener> listener,
      Optional<RateLimiter> rateLimiter,
      Optional<Semaphore> requestPermits,
      boolean failFast) {
    super(statement, session, executor, listener, rateLimiter, requestPermits, failFast);
  }

  @Override
  protected void consumePage(ResultSet rs) {
    assert rs.isFullyFetched();
    onNext(new DefaultWriteResult(statement, rs.getExecutionInfo()));
    onComplete();
  }

  @Override
  protected WriteResult toErrorResult(BulkExecutionException error) {
    return new DefaultWriteResult(error);
  }
}
