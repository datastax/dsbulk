/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api;

import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.oss.driver.api.core.session.Session;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Semaphore;

/** Base class for implementations of {@link BulkExecutor}. */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public abstract class AbstractBulkExecutor implements BulkExecutor, AutoCloseable {

  /** The default number of maximum in-flight requests. */
  static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 1_000;

  /** The default maximum number of concurrent requests per second. */
  static final int DEFAULT_MAX_REQUESTS_PER_SECOND = 100_000;

  protected final Session session;

  protected final boolean failFast;

  protected final Optional<Semaphore> requestPermits;

  protected final Optional<RateLimiter> rateLimiter;

  protected final Optional<ExecutionListener> listener;

  protected AbstractBulkExecutor(Session session) {
    this(session, true, DEFAULT_MAX_IN_FLIGHT_REQUESTS, DEFAULT_MAX_REQUESTS_PER_SECOND, null);
  }

  protected AbstractBulkExecutor(AbstractBulkExecutorBuilder<?> builder) {
    this(
        builder.session,
        builder.failFast,
        builder.maxInFlightRequests,
        builder.maxRequestsPerSecond,
        builder.listener);
  }

  private AbstractBulkExecutor(
      Session session,
      boolean failFast,
      int maxInFlightRequests,
      int maxRequestsPerSecond,
      ExecutionListener listener) {
    Objects.requireNonNull(session, "session cannot be null");
    this.session = session;
    this.failFast = failFast;
    this.requestPermits =
        maxInFlightRequests < 0
            ? Optional.empty()
            : Optional.of(new Semaphore(maxInFlightRequests));
    this.rateLimiter =
        maxRequestsPerSecond < 0
            ? Optional.empty()
            : Optional.of(RateLimiter.create(maxRequestsPerSecond));
    this.listener = Optional.ofNullable(listener);
  }

  @Override
  public void close() {
    // no-op
  }
}
