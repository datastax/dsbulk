/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api;

import static io.reactivex.BackpressureStrategy.BUFFER;

import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.emitter.ContinuousReadResultEmitter;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * An implementation of {@link BulkExecutor} using <a
 * href="https://github.com/ReactiveX/RxJava/wiki">RxJava</a>, that executes all reads using
 * continuous paging. This executor can achieve significant performance improvements for reads,
 * provided that the read statements to be executed can be properly routed to a replica.
 */
public class ContinuousRxJavaBulkExecutor extends DefaultRxJavaBulkExecutor
    implements RxJavaBulkExecutor {

  /**
   * Creates a new builder for {@link ContinuousRxJavaBulkExecutor} instances using the given {@link
   * ContinuousPagingSession}.
   *
   * @param session the {@link ContinuousPagingSession} to use.
   * @return a new builder.
   */
  public static ContinuousRxJavaBulkExecutorBuilder builder(ContinuousPagingSession session) {
    return new ContinuousRxJavaBulkExecutorBuilder(session);
  }

  private final ContinuousPagingSession session;
  private final ContinuousPagingOptions options;

  /**
   * Creates a new instance using the given {@link ContinuousPagingSession} and using defaults for
   * all parameters.
   *
   * <p>If you need to customize your executor, use the {@link #builder(ContinuousPagingSession)
   * builder} method instead.
   *
   * @param session the {@link ContinuousPagingSession} to use.
   */
  public ContinuousRxJavaBulkExecutor(ContinuousPagingSession session) {
    this(session, ContinuousPagingOptions.builder().build());
  }

  /**
   * Creates a new instance using the given {@link ContinuousPagingSession}, the given {@link
   * ContinuousPagingOptions}, and using defaults for all other parameters.
   *
   * <p>If you need to customize your executor, use the {@link #builder(ContinuousPagingSession)
   * builder} method instead.
   *
   * @param session the {@link ContinuousPagingSession} to use.
   * @param options the {@link ContinuousPagingOptions} to use.
   */
  public ContinuousRxJavaBulkExecutor(
      ContinuousPagingSession session, ContinuousPagingOptions options) {
    super(session);
    this.session = session;
    this.options = options;
  }

  ContinuousRxJavaBulkExecutor(
      ContinuousPagingSession session,
      ContinuousPagingOptions options,
      boolean failFast,
      int maxInFlightRequests,
      int maxRequestsPerSecond,
      ExecutionListener listener,
      Executor executor) {
    super(session, failFast, maxInFlightRequests, maxRequestsPerSecond, listener, executor);
    this.session = session;
    this.options = options;
  }

  @Override
  public Flowable<ReadResult> readReactive(Statement statement) {
    Objects.requireNonNull(statement);
    return Flowable.create(
        e -> {
          RxJavaContinuousReadResultEmitter emitter =
              new RxJavaContinuousReadResultEmitter(statement, e);
          emitter.start();
        },
        BUFFER);
  }

  private class RxJavaContinuousReadResultEmitter extends ContinuousReadResultEmitter {

    private final FlowableEmitter<ReadResult> emitter;

    private RxJavaContinuousReadResultEmitter(
        Statement statement, FlowableEmitter<ReadResult> emitter) {
      super(
          statement,
          ContinuousRxJavaBulkExecutor.this.session,
          options,
          executor,
          listener,
          rateLimiter,
          requestPermits,
          failFast);
      this.emitter = emitter;
    }

    @Override
    protected void notifyOnNext(ReadResult result) {
      emitter.onNext(result);
    }

    @Override
    protected void notifyOnComplete() {
      emitter.onComplete();
    }

    @Override
    protected void notifyOnError(BulkExecutionException error) {
      emitter.onError(error);
    }

    @Override
    protected boolean isCancelled() {
      return emitter.isCancelled();
    }
  }
}
