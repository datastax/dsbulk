/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.throttling;

/** A {@link RateLimiter} that establishes a fixed rate. */
public class FixedRateLimiter implements RateLimiter {

  private final com.google.common.util.concurrent.RateLimiter rateLimiter;

  public FixedRateLimiter(double permitsPerSecond) {
    if (permitsPerSecond < 1) {
      throw new IllegalArgumentException(
          "Expecting permitsPerSecond to be strictly positive, got " + permitsPerSecond);
    }
    this.rateLimiter = com.google.common.util.concurrent.RateLimiter.create(permitsPerSecond);
  }

  @Override
  public void acquire(int permits) {
    rateLimiter.acquire(permits);
  }
}
