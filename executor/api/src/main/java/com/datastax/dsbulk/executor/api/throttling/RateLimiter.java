/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.throttling;

/**
 * A rate limiter, inspired by Guava's {@link com.google.common.util.concurrent.RateLimiter
 * RateLimiter}.
 *
 * <p>Conceptually, a rate limiter distributes permits at a configurable rate. Each {@link
 * #acquire(int) acquire} method blocks if necessary until a permit is available, and then takes it.
 * Once acquired, permits need not be released.
 */
public interface RateLimiter {

  /**
   * The default {@link RateLimiter} is a {@link FixedRateLimiter} configured with a maximum
   * throughput of 100,000 requests per second.
   */
  RateLimiter DEFAULT = new FixedRateLimiter(100_000);

  /**
   * Acquires the given number of permits from this {@code RateLimiter}, blocking until the request
   * can be granted.
   *
   * @param permits the number of permits to acquire.
   * @throws IllegalArgumentException if the requested number of permits is negative or zero.
   */
  void acquire(int permits);
}
