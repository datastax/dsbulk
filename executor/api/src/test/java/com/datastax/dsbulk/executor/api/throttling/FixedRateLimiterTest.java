/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.throttling;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.RateLimiter;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

public class FixedRateLimiterTest {

  @Test
  public void should_create_fixed_rate_limiter_when_valid_permits() throws Exception {
    FixedRateLimiter rateLimiter = new FixedRateLimiter(42);
    RateLimiter rl = (RateLimiter) Whitebox.getInternalState(rateLimiter, "rateLimiter");
    assertThat(rl.getRate()).isEqualTo(42);
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_not_create_rate_limiter_when_invalid_permits() throws Exception {
    new FixedRateLimiter(0);
  }
}
