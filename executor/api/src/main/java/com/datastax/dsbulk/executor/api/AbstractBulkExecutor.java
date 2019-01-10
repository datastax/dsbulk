/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Semaphore;

/** Base class for implementations of {@link BulkExecutor}. */
@SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "UnstableApiUsage"})
public abstract class AbstractBulkExecutor implements BulkExecutor, AutoCloseable {

  /** The default number of maximum in-flight requests. */
  static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 1_000;

  /** The default number of maximum in-flight queries. */
  static final int DEFAULT_MAX_IN_FLIGHT_QUERIES = -1;

  /** The default maximum number of concurrent requests per second. */
  static final int DEFAULT_MAX_REQUESTS_PER_SECOND = 100_000;

  protected final Session session;

  protected final boolean failFast;

  protected final Optional<Semaphore> maxConcurrentRequests;

  protected final Optional<Semaphore> maxConcurrentQueries;

  protected final Optional<RateLimiter> rateLimiter;

  protected final Optional<ExecutionListener> listener;

  protected AbstractBulkExecutor(Session session) {
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
      Session session,
      boolean failFast,
      int maxInFlightRequests,
      int maxInFlightQueries,
      int maxRequestsPerSecond,
      ExecutionListener listener) {
    Objects.requireNonNull(session, "session cannot be null");
    this.session = session;
    this.failFast = failFast;
    this.maxConcurrentRequests =
        maxInFlightRequests <= 0
            ? Optional.empty()
            : Optional.of(new Semaphore(maxInFlightRequests));
    this.maxConcurrentQueries =
        maxInFlightQueries <= 0 ? Optional.empty() : Optional.of(new Semaphore(maxInFlightQueries));
    this.rateLimiter =
        maxRequestsPerSecond <= 0
            ? Optional.empty()
            : Optional.of(RateLimiter.create(maxRequestsPerSecond));
    this.listener = Optional.ofNullable(listener);
  }

  @Override
  public void close() {
    // no-op
  }
}
