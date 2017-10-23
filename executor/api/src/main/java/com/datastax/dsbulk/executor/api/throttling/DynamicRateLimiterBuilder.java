/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.throttling;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.DoublePredicate;
import java.util.function.DoubleSupplier;
import java.util.function.DoubleUnaryOperator;

/** A builder for {@link DynamicRateLimiter} instances. */
@SuppressWarnings({"UnusedReturnValue", "WeakerAccess"})
public class DynamicRateLimiterBuilder {

  private final double initialPermitsPerSecond;
  private DoubleSupplier rateProvider;
  private DoubleSupplier statProvider;
  private DoublePredicate isStatTooHigh;
  private DoublePredicate isStatTooLow;
  private DoubleUnaryOperator onStatTooHigh;
  private DoubleUnaryOperator onStatTooLow;
  private long checkIntervalNanos;
  private long warmUpPeriodNanos;

  /**
   * Creates a new builder with the given initial number of permits per second.
   *
   * @param initialPermitsPerSecond the initial number of permits per second; must be strictly
   *     positive.
   * @throws IllegalArgumentException if the number of permits is &lt; 1.
   */
  DynamicRateLimiterBuilder(double initialPermitsPerSecond) {
    if (initialPermitsPerSecond < 1) {
      throw new IllegalArgumentException(
          "Expecting initialPermitsPerSecond to be strictly positive, got "
              + initialPermitsPerSecond);
    }
    this.initialPermitsPerSecond = initialPermitsPerSecond;
  }

  /**
   * Sets the rate provider.
   *
   * <p>The supplier will be called every time a check is made to determine whether or not the
   * current rate should be adjusted.
   *
   * @param rateProvider the rate provider.
   * @return this builder (for method chaining).
   * @throws NullPointerException if the supplied argument is {@code null}.
   */
  public DynamicRateLimiterBuilder withRateProvider(DoubleSupplier rateProvider) {
    Objects.requireNonNull(rateProvider, "rateProvider cannot be null");
    this.rateProvider = rateProvider;
    return this;
  }

  /**
   * Sets the statistics provider.
   *
   * <p>The supplier will be called every time a check is made to determine whether or not the
   * current rate should be adjusted.
   *
   * <p>Every value supplied will be tested by two predicates: {@link
   * #withStatTooHighPredicate(DoublePredicate) too high} and {@link
   * #withStatTooLowPredicate(DoublePredicate) too low}. If any of these tests is positive, a new
   * rate will be computed by two functions: {@link #withOnStatTooHigh(DoubleUnaryOperator)
   * onStatTooHigh} or {@link #withOnStatTooLow(DoubleUnaryOperator) onStatTooLow} respectively.
   *
   * @param statProvider the statistics provider.
   * @return this builder (for method chaining).
   * @throws NullPointerException if the supplied argument is {@code null}.
   */
  public DynamicRateLimiterBuilder withStatProvider(DoubleSupplier statProvider) {
    Objects.requireNonNull(statProvider, "statProvider cannot be null");
    this.statProvider = statProvider;
    return this;
  }

  /**
   * Creates two constant predicates to evaluate the current stat, one that tests if the current
   * stat is below {@code minStat}, and another that tests if the current stat is above {@code
   * maxStat}.
   *
   * @param minStat the minimum threshold
   * @param maxStat the maximum threshold
   * @return this builder (for method chaining).
   * @throws IllegalArgumentException if {@code minStat} &lt; 1 or {@code maxStat} &lt; 1 or {@code
   *     maxStat} &lt;= {@code minStat}.
   */
  public DynamicRateLimiterBuilder withFixedThresholds(double minStat, double maxStat) {
    if (minStat < 1) {
      throw new IllegalArgumentException(
          "Expecting minStat to be strictly positive, got " + minStat);
    }
    if (maxStat < 1) {
      throw new IllegalArgumentException(
          "Expecting maxStat to be strictly positive, got " + maxStat);
    }
    if (maxStat <= minStat) {
      throw new IllegalArgumentException(
          String.format(
              "Expecting maxStat to be greater than minStat, got min = %s and max = %s",
              minStat, maxStat));
    }
    withStatTooLowPredicate(currStat -> currStat < minStat);
    withStatTooHighPredicate(currStat -> currStat > maxStat);
    return this;
  }

  /**
   * Sets the {@link DoublePredicate predicate} to use to evaluate if the current stat is too low.
   *
   * @param isStatTooLow the {@link DoublePredicate predicate} to use to evaluate if the current
   *     stat is too low.
   * @return this builder (for method chaining).
   * @throws NullPointerException if the supplied argument is {@code null}.
   */
  public DynamicRateLimiterBuilder withStatTooLowPredicate(DoublePredicate isStatTooLow) {
    Objects.requireNonNull(isStatTooLow, "isStatTooLow cannot be null");
    this.isStatTooLow = isStatTooLow;
    return this;
  }

  /**
   * Sets the {@link DoublePredicate predicate} to use to evaluate if the current stat is too high.
   *
   * @param isStatTooHigh the {@link DoublePredicate predicate} to use to evaluate if the current
   *     stat is too high.
   * @return this builder (for method chaining).
   * @throws NullPointerException if the supplied argument is {@code null}.
   */
  public DynamicRateLimiterBuilder withStatTooHighPredicate(DoublePredicate isStatTooHigh) {
    Objects.requireNonNull(isStatTooHigh, "isStatTooHigh cannot be null");
    this.isStatTooHigh = isStatTooHigh;
    return this;
  }

  /**
   * Applies a constant back-off to the current rate when the current stat is too low or too high.
   *
   * @param addend the number of permits to add or subtract to the current rate; must be strictly
   *     positive.
   * @return this builder (for method chaining).
   * @throws IllegalArgumentException if {@code addend} &lt; 1.
   */
  public DynamicRateLimiterBuilder withConstantBackOff(double addend) {
    if (addend < 1) {
      throw new IllegalArgumentException("Expecting addend to be strictly positive, got " + addend);
    }
    withOnStatTooHigh(currRate -> Math.max(1, currRate - addend));
    withOnStatTooLow(currRate -> currRate + addend);
    return this;
  }

  /**
   * Applies an exponential back-off to the current rate when the current stat is too low or too
   * high.
   *
   * @param upMultiplier the fraction of the current rate to add to it when adjusting up; must be
   *     &gt; 0 and &lt; 1.
   * @param downMultiplier the fraction of the current rate to subtract from it when adjusting down;
   *     must be &gt; 0 and &lt; 1.
   * @return this builder (for method chaining).
   * @throws IllegalArgumentException if {@code upMultiplier} &lt;= 0 or &gt;= 1, or if {@code
   *     downMultiplier} &lt;= 0 or &gt;= 1.
   */
  public DynamicRateLimiterBuilder withExponentialBackOff(
      double upMultiplier, double downMultiplier) {
    if (upMultiplier >= 1 || upMultiplier <= 0) {
      throw new IllegalArgumentException(
          "Expecting upMultiplier to be > 0 and < 1, got " + upMultiplier);
    }
    if (downMultiplier >= 1 || downMultiplier <= 0) {
      throw new IllegalArgumentException(
          "Expecting downMultiplier to be > 0 and < 1, got " + downMultiplier);
    }
    withOnStatTooHigh(currRate -> currRate - (currRate * downMultiplier));
    withOnStatTooLow(currRate -> currRate + (currRate * upMultiplier));
    return this;
  }

  /**
   * Sets the adjustment to apply when the current rate is too high.
   *
   * <p>The given function will be invoked with the current rate and is expected to return the new
   * rate to apply.
   *
   * @param onStatTooHigh the adjustment function.
   * @return this builder (for method chaining).
   * @throws NullPointerException if the supplied argument is {@code null}.
   */
  public DynamicRateLimiterBuilder withOnStatTooHigh(DoubleUnaryOperator onStatTooHigh) {
    Objects.requireNonNull(onStatTooHigh, "onStatTooHigh cannot be null");
    this.onStatTooHigh = onStatTooHigh;
    return this;
  }

  /**
   * Sets the adjustment to apply when the current rate is too low.
   *
   * <p>The given function will be invoked with the current rate and is expected to return the new
   * rate to apply.
   *
   * @param onStatTooLow the adjustment function.
   * @return this builder (for method chaining).
   * @throws NullPointerException if the supplied argument is {@code null}.
   */
  public DynamicRateLimiterBuilder withOnStatTooLow(DoubleUnaryOperator onStatTooLow) {
    Objects.requireNonNull(onStatTooLow, "onStatTooLow cannot be null");
    this.onStatTooLow = onStatTooLow;
    return this;
  }

  /**
   * Sets the check interval.
   *
   * <p>The current rate will be tested and potentially adjusted every time this interval is
   * elapsed.
   *
   * @param interval the check interval.
   * @param timeUnit the time unit.
   * @return this builder (for method chaining).
   * @throws IllegalArgumentException if the {@code interval} is &lt; 1.
   * @throws NullPointerException if the {@code timeUnit} is {@code null}.
   */
  public DynamicRateLimiterBuilder withCheckInterval(long interval, TimeUnit timeUnit) {
    if (interval < 1) {
      throw new IllegalArgumentException(
          "Expecting interval to be strictly positive, got " + interval);
    }
    Objects.requireNonNull(timeUnit, "timeUnit cannot be null");
    this.checkIntervalNanos = timeUnit.toNanos(interval);
    return this;
  }

  /**
   * Sets the check interval.
   *
   * <p>The current rate will be tested and potentially adjusted every time this interval is
   * elapsed.
   *
   * @param interval the check interval.
   * @return this builder (for method chaining).
   * @throws NullPointerException if the {@code interval} is {@code null}.
   * @throws IllegalArgumentException if the {@code interval} is negative or zero.
   */
  public DynamicRateLimiterBuilder withCheckInterval(Duration interval) {
    Objects.requireNonNull(interval, "interval cannot be null");
    if (interval.isNegative() || interval.isZero()) {
      throw new IllegalArgumentException(
          "Expecting period to be strictly positive, got " + interval);
    }
    this.checkIntervalNanos = interval.toNanos();
    return this;
  }

  /**
   * Sets the warm-up period.
   *
   * <p>The current rate will be tested and potentially adjusted only after this period is elapsed.
   *
   * @param period the warm-up period.
   * @param timeUnit the time unit.
   * @return this builder (for method chaining).
   * @throws IllegalArgumentException if the {@code period} is &lt; 0.
   * @throws NullPointerException if the {@code timeUnit} is {@code null}.
   */
  public DynamicRateLimiterBuilder withWarmUpPeriod(long period, TimeUnit timeUnit) {
    if (period < 0) {
      throw new IllegalArgumentException("Expecting period to be positive, got " + period);
    }
    Objects.requireNonNull(timeUnit, "timeUnit cannot be null");
    this.warmUpPeriodNanos = timeUnit.toNanos(period);
    return this;
  }

  /**
   * Sets the warm-up period.
   *
   * <p>The current rate will be tested and potentially adjusted only after this period is elapsed.
   *
   * @param period the warm-up period.
   * @return this builder (for method chaining).
   * @throws NullPointerException if the {@code period} is {@code null}.
   * @throws IllegalArgumentException if the {@code period} is negative.
   */
  public DynamicRateLimiterBuilder withWarmUpPeriod(Duration period) {
    Objects.requireNonNull(period, "period cannot be null");
    if (period.isNegative()) {
      throw new IllegalArgumentException("Expecting period to be positive, got " + period);
    }
    this.warmUpPeriodNanos = period.toNanos();
    return this;
  }

  /**
   * Builds a new instance of {@link DynamicRateLimiter}.
   *
   * @return the newly-allocated {@link DynamicRateLimiter} instance.
   */
  public DynamicRateLimiter build() {
    return new DynamicRateLimiter(
        initialPermitsPerSecond,
        rateProvider,
        statProvider,
        isStatTooHigh,
        isStatTooLow,
        onStatTooHigh,
        onStatTooLow,
        checkIntervalNanos,
        warmUpPeriodNanos);
  }
}
