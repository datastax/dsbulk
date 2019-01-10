/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.reactor;

import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.BulkExecutor;
import com.datastax.dsbulk.executor.api.internal.publisher.ContinuousReadResultPublisher;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import java.util.Objects;
import reactor.core.publisher.Flux;

/**
 * An implementation of {@link BulkExecutor} using <a href="https://projectreactor.io">Reactor</a>,
 * that executes all reads using continuous paging. This executor can achieve significant
 * performance improvements for reads, provided that the read statements to be executed can be
 * properly routed to a replica.
 */
public class ContinuousReactorBulkExecutor extends DefaultReactorBulkExecutor
    implements ReactorBulkExecutor {

  /**
   * Creates a new builder for {@link ContinuousReactorBulkExecutor} instances using the given
   * {@link ContinuousPagingSession}.
   *
   * @param session the {@link ContinuousPagingSession} to use.
   * @return a new builder.
   */
  public static ContinuousReactorBulkExecutorBuilder builder(ContinuousPagingSession session) {
    return new ContinuousReactorBulkExecutorBuilder(session);
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
  public ContinuousReactorBulkExecutor(ContinuousPagingSession continuousPagingSession) {
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
  public ContinuousReactorBulkExecutor(
      ContinuousPagingSession continuousPagingSession,
      ContinuousPagingOptions continuousPagingOptions) {
    super(continuousPagingSession);
    this.continuousPagingSession = continuousPagingSession;
    this.continuousPagingOptions = continuousPagingOptions;
  }

  ContinuousReactorBulkExecutor(ContinuousReactorBulkExecutorBuilder builder) {
    super(builder);
    this.continuousPagingSession = builder.continuousPagingSession;
    this.continuousPagingOptions = builder.options;
  }

  @Override
  public Flux<ReadResult> readReactive(Statement statement) {
    Objects.requireNonNull(statement);
    return Flux.from(
        new ContinuousReadResultPublisher(
            statement,
            continuousPagingSession,
            continuousPagingOptions,
            failFast,
            listener,
            requestPermits,
            queryPermits,
            rateLimiter));
  }
}
