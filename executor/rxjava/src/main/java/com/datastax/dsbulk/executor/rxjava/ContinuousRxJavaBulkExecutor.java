/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.rxjava;

import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.BulkExecutor;
import com.datastax.dsbulk.executor.api.internal.publisher.ContinuousReadResultPublisher;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import io.reactivex.Flowable;
import java.util.Objects;

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

  private final ContinuousPagingSession continuousPagingSession;
  private final ContinuousPagingOptions continuousPagingOptions;

  /**
   * Creates a new instance using the given {@link ContinuousPagingSession} and using defaults for
   * all parameters.
   *
   * <p>If you need to customize your executor, use the {@link #builder(ContinuousPagingSession)
   * builder} method instead.
   *
   * @param continuousPagingSession the {@link ContinuousPagingSession} to use.
   */
  public ContinuousRxJavaBulkExecutor(ContinuousPagingSession continuousPagingSession) {
    this(continuousPagingSession, ContinuousPagingOptions.builder().build());
  }

  /**
   * Creates a new instance using the given {@link ContinuousPagingSession}, the given {@link
   * ContinuousPagingOptions}, and using defaults for all other parameters.
   *
   * <p>If you need to customize your executor, use the {@link #builder(ContinuousPagingSession)
   * builder} method instead.
   *
   * @param continuousPagingSession the {@link ContinuousPagingSession} to use.
   * @param continuousPagingOptions the {@link ContinuousPagingOptions} to use.
   */
  public ContinuousRxJavaBulkExecutor(
      ContinuousPagingSession continuousPagingSession,
      ContinuousPagingOptions continuousPagingOptions) {
    super(continuousPagingSession);
    this.continuousPagingSession = continuousPagingSession;
    this.continuousPagingOptions = continuousPagingOptions;
  }

  ContinuousRxJavaBulkExecutor(ContinuousRxJavaBulkExecutorBuilder builder) {
    super(builder);
    this.continuousPagingSession = builder.continuousPagingSession;
    this.continuousPagingOptions = builder.options;
  }

  @Override
  public Flowable<ReadResult> readReactive(Statement statement) {
    Objects.requireNonNull(statement);
    return Flowable.fromPublisher(
        new ContinuousReadResultPublisher(
            statement,
            continuousPagingSession,
            continuousPagingOptions,
            failFast,
            listener,
            requestPermits,
            rateLimiter));
  }
}
