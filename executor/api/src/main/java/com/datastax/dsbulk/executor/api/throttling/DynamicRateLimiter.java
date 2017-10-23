/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.throttling;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoublePredicate;
import java.util.function.DoubleSupplier;
import java.util.function.DoubleUnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A dynamic {@link RateLimiter} that adjusts its rate when it gets too high or too low. */
public class DynamicRateLimiter implements RateLimiter {

  /**
   * Creates a new {@link DynamicRateLimiterBuilder} with the given initial number of permits per
   * second.
   *
   * @param initialPermitsPerSecond the initial number of permits per second; must be strictly
   *     positive.
   * @throws IllegalArgumentException if the number of permits is &lt; 1.
   */
  public static DynamicRateLimiterBuilder builder(double initialPermitsPerSecond) {
    return new DynamicRateLimiterBuilder(initialPermitsPerSecond);
  }

  private enum State {
    CHECKING,
    WAITING_DOWN,
    WAITING_UP
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamicRateLimiter.class);

  private final AtomicReference<State> state = new AtomicReference<>(State.CHECKING);

  private final com.google.common.util.concurrent.RateLimiter rateLimiter;

  private final DoubleSupplier rateProvider;
  private final DoubleSupplier statProvider;
  private final DoublePredicate isStatTooHigh;
  private final DoublePredicate isStatTooLow;
  private final DoubleUnaryOperator onStatTooHigh;
  private final DoubleUnaryOperator onStatTooLow;

  private final long checkIntervalNanos;
  private final AtomicLong lastCheckNanos;

  DynamicRateLimiter(
      double initialPermitsPerSecond,
      DoubleSupplier rateProvider,
      DoubleSupplier statProvider,
      DoublePredicate isStatTooHigh,
      DoublePredicate isStatTooLow,
      DoubleUnaryOperator onStatTooHigh,
      DoubleUnaryOperator onStatTooLow,
      long checkIntervalNanos,
      long warmUpPeriodNanos) {
    if (initialPermitsPerSecond < 1) {
      throw new IllegalArgumentException(
          "Expecting initialPermitsPerSecond to be strictly positive, got "
              + initialPermitsPerSecond);
    }
    if (checkIntervalNanos < 1) {
      throw new IllegalArgumentException(
          "Expecting checkIntervalNanos to be strictly positive, got " + checkIntervalNanos);
    }
    Objects.requireNonNull(rateProvider, "rateProvider cannot be null");
    Objects.requireNonNull(statProvider, "statProvider cannot be null");
    Objects.requireNonNull(isStatTooHigh, "isStatTooHigh cannot be null");
    Objects.requireNonNull(isStatTooLow, "isStatTooLow cannot be null");
    Objects.requireNonNull(onStatTooHigh, "onStatTooHigh cannot be null");
    Objects.requireNonNull(onStatTooLow, "onStatTooLow cannot be null");
    this.rateLimiter =
        com.google.common.util.concurrent.RateLimiter.create(initialPermitsPerSecond);
    this.rateProvider = rateProvider;
    this.statProvider = statProvider;
    this.isStatTooHigh = isStatTooHigh;
    this.isStatTooLow = isStatTooLow;
    this.onStatTooHigh = onStatTooHigh;
    this.onStatTooLow = onStatTooLow;
    this.checkIntervalNanos = checkIntervalNanos;
    lastCheckNanos = new AtomicLong(System.nanoTime() + warmUpPeriodNanos - checkIntervalNanos);
  }

  @Override
  public void acquire(int permits) {
    switch (state.get()) {
      case CHECKING:
        check();
        break;
      case WAITING_DOWN:
        waitDown();
        break;
      case WAITING_UP:
        waitUp();
        break;
    }
    rateLimiter.acquire(permits);
  }

  private void check() {
    long now = System.nanoTime();
    long last = lastCheckNanos.get();
    if (now - (last + checkIntervalNanos) > 0 && lastCheckNanos.compareAndSet(last, now)) {
      maybeAdjustRate();
    }
  }

  private void waitDown() {
    double currRate = rateProvider.getAsDouble();
    double targetRate = rateLimiter.getRate();
    if (currRate <= targetRate) {
      if (state.compareAndSet(State.WAITING_DOWN, State.CHECKING)) {
        check();
      }
    }
  }

  private void waitUp() {
    double currRate = rateProvider.getAsDouble();
    double targetRate = rateLimiter.getRate();
    if (currRate >= targetRate) {
      if (state.compareAndSet(State.WAITING_UP, State.CHECKING)) {
        check();
      }
    }
  }

  private void maybeAdjustRate() {
    double currStat = statProvider.getAsDouble();
    double currRate = rateProvider.getAsDouble();
    double newRate = currRate;
    if (isStatTooHigh.test(currStat)) {
      newRate = onStatTooHigh.applyAsDouble(currRate);
    } else if (isStatTooLow.test(currStat)) {
      newRate = onStatTooLow.applyAsDouble(currRate);
    }
    if (currRate != newRate) {
      State newState = currRate < newRate ? State.WAITING_UP : State.WAITING_DOWN;
      if (state.compareAndSet(State.CHECKING, newState)) {
        rateLimiter.setRate(newRate);
        LOGGER.warn("Adjusting rate: {} -> {}", currRate, newRate);
      }
    }
  }
}
