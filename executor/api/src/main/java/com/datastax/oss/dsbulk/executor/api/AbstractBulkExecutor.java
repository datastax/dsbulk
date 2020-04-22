/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.executor.api;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import com.datastax.oss.dsbulk.executor.api.listener.ExecutionListener;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import java.util.concurrent.Semaphore;

/** Base class for implementations of {@link BulkExecutor}. */
public abstract class AbstractBulkExecutor implements BulkExecutor, AutoCloseable {

  /** The default number of maximum in-flight requests. */
  static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 1_000;

  /** The default number of maximum in-flight queries. */
  static final int DEFAULT_MAX_IN_FLIGHT_QUERIES = -1;

  /** The default maximum number of concurrent requests per second. */
  static final int DEFAULT_MAX_REQUESTS_PER_SECOND = 100_000;

  protected final @NonNull CqlSession session;

  protected final boolean failFast;

  protected final @Nullable Semaphore maxConcurrentRequests;

  protected final @Nullable Semaphore maxConcurrentQueries;

  protected final @Nullable RateLimiter rateLimiter;

  protected final @Nullable ExecutionListener listener;

  protected AbstractBulkExecutor(CqlSession session) {
    this(
        session,
        true,
        DEFAULT_MAX_IN_FLIGHT_REQUESTS,
        DEFAULT_MAX_IN_FLIGHT_QUERIES,
        DEFAULT_MAX_REQUESTS_PER_SECOND,
        null);
  }

  protected AbstractBulkExecutor(AbstractBulkExecutorBuilder<?> builder) {
    this(
        builder.session,
        builder.failFast,
        builder.maxInFlightRequests,
        builder.maxInFlightQueries,
        builder.maxRequestsPerSecond,
        builder.listener);
  }

  private AbstractBulkExecutor(
      @NonNull CqlSession session,
      boolean failFast,
      int maxInFlightRequests,
      int maxInFlightQueries,
      int maxRequestsPerSecond,
      @Nullable ExecutionListener listener) {
    Objects.requireNonNull(session, "session cannot be null");
    this.session = session;
    this.failFast = failFast;
    this.maxConcurrentRequests =
        maxInFlightRequests <= 0 ? null : new Semaphore(maxInFlightRequests);
    this.maxConcurrentQueries = maxInFlightQueries <= 0 ? null : new Semaphore(maxInFlightQueries);
    this.rateLimiter = maxRequestsPerSecond <= 0 ? null : RateLimiter.create(maxRequestsPerSecond);
    this.listener = listener;
  }

  @Override
  public void close() {
    // no-op
  }
}
