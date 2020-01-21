/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.reactor;

import com.datastax.dsbulk.executor.api.BulkExecutor;
import com.datastax.dsbulk.executor.api.internal.publisher.ContinuousReadResultPublisher;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Statement;
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
   * {@link CqlSession}.
   *
   * @param session the {@link CqlSession} to use.
   * @return a new builder.
   */
  public static ContinuousReactorBulkExecutorBuilder builderCP(CqlSession session) {
    return new ContinuousReactorBulkExecutorBuilder(session);
  }

  private final CqlSession cqlSession;

  /**
   * Creates a new instance using the given {@link CqlSession} and using defaults for all
   * parameters.
   *
   * <p>If you need to customize your executor, use the {@link #builder(CqlSession) builder} method
   * instead.
   *
   * @param cqlSession the {@link CqlSession} to use.
   */
  public ContinuousReactorBulkExecutor(CqlSession cqlSession) {
    super(cqlSession);
    this.cqlSession = cqlSession;
  }

  ContinuousReactorBulkExecutor(ContinuousReactorBulkExecutorBuilder builder) {
    super(builder);
    this.cqlSession = builder.cqlSession;
  }

  @Override
  public Flux<ReadResult> readReactive(Statement<?> statement) {
    Objects.requireNonNull(statement);
    return Flux.from(
        new ContinuousReadResultPublisher(
            statement,
            cqlSession,
            failFast,
            listener,
            maxConcurrentRequests,
            maxConcurrentQueries,
            rateLimiter));
  }
}
