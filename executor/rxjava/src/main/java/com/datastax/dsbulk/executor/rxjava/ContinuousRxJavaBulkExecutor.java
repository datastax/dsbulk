/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.rxjava;

import com.datastax.dsbulk.executor.api.BulkExecutor;
import com.datastax.dsbulk.executor.api.internal.publisher.ContinuousReadResultPublisher;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.cql.Statement;
import io.reactivex.Flowable;
import java.util.Objects;

/**
 * An implementation of {@link BulkExecutor} using <a href="https://projectrxJava.io">RxJava</a>,
 * that executes all reads using continuous paging. This executor can achieve significant
 * performance improvements for reads, provided that the read statements to be executed can be
 * properly routed to a replica.
 */
public class ContinuousRxJavaBulkExecutor extends DefaultRxJavaBulkExecutor
    implements RxJavaBulkExecutor {

  /**
   * Creates a new builder for {@link ContinuousRxJavaBulkExecutor} instances using the given {@link
   * DseSession}.
   *
   * @param session the {@link DseSession} to use.
   * @return a new builder.
   */
  public static ContinuousRxJavaBulkExecutorBuilder builder(DseSession session) {
    return new ContinuousRxJavaBulkExecutorBuilder(session);
  }

  private final DseSession dseSession;

  /**
   * Creates a new instance using the given {@link DseSession} and using defaults for all
   * parameters.
   *
   * <p>If you need to customize your executor, use the {@link #builder(DseSession) builder} method
   * instead.
   *
   * @param dseSession the {@link DseSession} to use.
   */
  public ContinuousRxJavaBulkExecutor(DseSession dseSession) {
    super(dseSession);
    this.dseSession = dseSession;
  }

  ContinuousRxJavaBulkExecutor(ContinuousRxJavaBulkExecutorBuilder builder) {
    super(builder);
    this.dseSession = builder.dseSession;
  }

  @Override
  public Flowable<ReadResult> readReactive(Statement statement) {
    Objects.requireNonNull(statement);
    return Flowable.fromPublisher(
        new ContinuousReadResultPublisher(
            statement,
            dseSession,
            failFast,
            listener,
            maxConcurrentRequests,
            maxConcurrentQueries,
            rateLimiter));
  }
}
