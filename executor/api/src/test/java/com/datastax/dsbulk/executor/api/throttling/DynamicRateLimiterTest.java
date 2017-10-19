/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.throttling;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.RateLimiter;
import java.time.Duration;
import java.util.function.DoubleSupplier;
import org.assertj.core.data.Offset;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

public class DynamicRateLimiterTest {

  @Test
  public void should_create_fixed_rate_limiter_when_valid_permits() throws Exception {
    DoubleSupplier statProvider = mock(DoubleSupplier.class);
    DoubleSupplier rateProvider = mock(DoubleSupplier.class);
    DynamicRateLimiter rateLimiter =
        DynamicRateLimiter.builder(100)
            .withCheckInterval(Duration.ofNanos(1))
            .withStatProvider(statProvider)
            .withRateProvider(rateProvider)
            .withStatTooHighPredicate(stat -> stat > 10)
            .withStatTooLowPredicate(stat -> stat < 5)
            .withOnStatTooHigh(rate -> rate - 1)
            .withOnStatTooLow(rate -> rate + 1)
            .build();
    Thread.sleep(1);
    RateLimiter rl = (RateLimiter) Whitebox.getInternalState(rateLimiter, "rateLimiter");
    assertThat(rl.getRate()).isEqualTo(100);
    when(statProvider.getAsDouble()).thenReturn(11d);
    when(rateProvider.getAsDouble()).thenReturn(100d);
    rateLimiter.acquire(1);
    assertThat(rl.getRate()).isEqualTo(99);
    when(statProvider.getAsDouble()).thenReturn(4d);
    when(rateProvider.getAsDouble()).thenReturn(99d);
    rateLimiter.acquire(1);
    assertThat(rl.getRate()).isEqualTo(100);
  }

  @Test
  public void should_create_fixed_rate_limiter_when_valid_permits_with_constant_backoff()
      throws Exception {
    DoubleSupplier statProvider = mock(DoubleSupplier.class);
    DoubleSupplier rateProvider = mock(DoubleSupplier.class);
    DynamicRateLimiter rateLimiter =
        DynamicRateLimiter.builder(100)
            .withCheckInterval(Duration.ofNanos(1))
            .withStatProvider(statProvider)
            .withRateProvider(rateProvider)
            .withFixedThresholds(5, 10)
            .withConstantBackOff(1)
            .build();
    Thread.sleep(1);
    RateLimiter rl = (RateLimiter) Whitebox.getInternalState(rateLimiter, "rateLimiter");
    assertThat(rl.getRate()).isEqualTo(100);
    when(statProvider.getAsDouble()).thenReturn(11d);
    when(rateProvider.getAsDouble()).thenReturn(100d);
    rateLimiter.acquire(1);
    assertThat(rl.getRate()).isEqualTo(99);
    when(statProvider.getAsDouble()).thenReturn(4d);
    when(rateProvider.getAsDouble()).thenReturn(99d);
    rateLimiter.acquire(1);
    assertThat(rl.getRate()).isEqualTo(100);
  }

  @Test
  public void should_create_fixed_rate_limiter_when_valid_permits_with_exponential_backoff()
      throws Exception {
    DoubleSupplier statProvider = mock(DoubleSupplier.class);
    DoubleSupplier rateProvider = mock(DoubleSupplier.class);
    DynamicRateLimiter rateLimiter =
        DynamicRateLimiter.builder(100)
            .withCheckInterval(1, NANOSECONDS)
            .withStatProvider(statProvider)
            .withRateProvider(rateProvider)
            .withFixedThresholds(5, 10)
            .withExponentialBackOff(0.1, 0.5)
            .build();
    Thread.sleep(1);
    RateLimiter rl = (RateLimiter) Whitebox.getInternalState(rateLimiter, "rateLimiter");
    assertThat(rl.getRate()).isEqualTo(100);
    when(statProvider.getAsDouble()).thenReturn(11d);
    when(rateProvider.getAsDouble()).thenReturn(100d);
    rateLimiter.acquire(1);
    assertThat(rl.getRate()).isEqualTo(50);
    when(statProvider.getAsDouble()).thenReturn(4d);
    when(rateProvider.getAsDouble()).thenReturn(50d);
    rateLimiter.acquire(1);
    assertThat(rl.getRate()).isEqualTo(55, Offset.offset(0.1));
  }

  @Test
  public void should_not_adjust_rate_before_check_interval() throws Exception {
    DoubleSupplier statProvider = mock(DoubleSupplier.class);
    DoubleSupplier rateProvider = mock(DoubleSupplier.class);
    DynamicRateLimiter rateLimiter =
        DynamicRateLimiter.builder(100)
            .withCheckInterval(10, SECONDS)
            .withStatProvider(statProvider)
            .withRateProvider(rateProvider)
            .withFixedThresholds(5, 10)
            .withExponentialBackOff(0.1, 0.5)
            .build();
    RateLimiter rl = (RateLimiter) Whitebox.getInternalState(rateLimiter, "rateLimiter");
    assertThat(rl.getRate()).isEqualTo(100);
    when(statProvider.getAsDouble()).thenReturn(11d);
    when(rateProvider.getAsDouble()).thenReturn(100d);
    rateLimiter.acquire(1);
    assertThat(rl.getRate()).isEqualTo(50);
    when(statProvider.getAsDouble()).thenReturn(4d);
    when(rateProvider.getAsDouble()).thenReturn(50d);
    rateLimiter.acquire(1); // no check
    assertThat(rl.getRate()).isEqualTo(50);
    rateLimiter =
        DynamicRateLimiter.builder(100)
            .withCheckInterval(Duration.ofSeconds(10))
            .withStatProvider(statProvider)
            .withRateProvider(rateProvider)
            .withFixedThresholds(5, 10)
            .withExponentialBackOff(0.1, 0.5)
            .build();
    rl = (RateLimiter) Whitebox.getInternalState(rateLimiter, "rateLimiter");
    assertThat(rl.getRate()).isEqualTo(100);
    when(statProvider.getAsDouble()).thenReturn(11d);
    when(rateProvider.getAsDouble()).thenReturn(100d);
    rateLimiter.acquire(1);
    assertThat(rl.getRate()).isEqualTo(50);
    when(statProvider.getAsDouble()).thenReturn(4d);
    when(rateProvider.getAsDouble()).thenReturn(50d);
    rateLimiter.acquire(1); // no check
    assertThat(rl.getRate()).isEqualTo(50);
  }

  @Test
  public void should_not_adjust_rate_before_warm_up_period() throws Exception {
    DoubleSupplier statProvider = mock(DoubleSupplier.class);
    DoubleSupplier rateProvider = mock(DoubleSupplier.class);
    DynamicRateLimiter rateLimiter =
        DynamicRateLimiter.builder(100)
            .withCheckInterval(1, NANOSECONDS)
            .withStatProvider(statProvider)
            .withRateProvider(rateProvider)
            .withFixedThresholds(5, 10)
            .withExponentialBackOff(0.1, 0.5)
            .withWarmUpPeriod(10, SECONDS)
            .build();
    RateLimiter rl = (RateLimiter) Whitebox.getInternalState(rateLimiter, "rateLimiter");
    assertThat(rl.getRate()).isEqualTo(100);
    when(statProvider.getAsDouble()).thenReturn(11d);
    when(rateProvider.getAsDouble()).thenReturn(100d);
    rateLimiter.acquire(1); // no check
    assertThat(rl.getRate()).isEqualTo(100);
    rateLimiter =
        DynamicRateLimiter.builder(100)
            .withCheckInterval(Duration.ofNanos(1))
            .withStatProvider(statProvider)
            .withRateProvider(rateProvider)
            .withFixedThresholds(5, 10)
            .withExponentialBackOff(0.1, 0.5)
            .withWarmUpPeriod(Duration.ofSeconds(10))
            .build();
    rl = (RateLimiter) Whitebox.getInternalState(rateLimiter, "rateLimiter");
    assertThat(rl.getRate()).isEqualTo(100);
    when(statProvider.getAsDouble()).thenReturn(11d);
    when(rateProvider.getAsDouble()).thenReturn(100d);
    rateLimiter.acquire(1); // no check
    assertThat(rl.getRate()).isEqualTo(100);
  }
}
