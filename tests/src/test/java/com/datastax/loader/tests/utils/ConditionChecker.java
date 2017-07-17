/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.tests.utils;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConditionChecker {

  private static final Logger logger = LoggerFactory.getLogger(ConditionChecker.class);

  private static final int DEFAULT_PERIOD_MILLIS = 500;
  private static final int DEFAULT_TIMEOUT_MILLIS = 60000;

  private final Object input;
  private final Predicate<Object> predicate;
  private final Lock lock;
  private final Condition condition;
  private final Timer timer;

  @SuppressWarnings("unchecked")
  public <T> ConditionChecker(
      T input, Predicate<? super T> predicate, long period, TimeUnit periodUnit) {
    this.input = input;
    this.predicate = (Predicate<Object>) predicate;
    lock = new ReentrantLock();
    condition = lock.newCondition();
    timer = new Timer("condition-checker", true);
    timer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            checkCondition();
          }
        },
        0,
        periodUnit.toMillis(period));
  }

  public static ConditionCheckerBuilder check() {
    return new ConditionCheckerBuilder();
  }

  /** Waits until the predicate becomes true, or a timeout occurs, whichever happens first. */
  public void await(long timeout, TimeUnit unit) {
    boolean interrupted = false;
    long nanos = unit.toNanos(timeout);
    lock.lock();
    try {
      while (!evalCondition()) {
        if (nanos <= 0L)
          throw new AssertionError(
              String.format(
                  "Timeout after %s %s while waiting for condition",
                  timeout, unit.toString().toLowerCase()));
        try {
          nanos = condition.awaitNanos(nanos);
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      timer.cancel();
      if (interrupted) Thread.currentThread().interrupt();
    }
  }

  private void checkCondition() {
    lock.lock();
    try {
      if (evalCondition()) {
        condition.signal();
      }
    } finally {
      lock.unlock();
    }
  }

  private boolean evalCondition() {
    return predicate.test(input);
  }

  public static class ConditionCheckerBuilder {

    private long timeout = DEFAULT_TIMEOUT_MILLIS;

    private TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;

    private long period = DEFAULT_PERIOD_MILLIS;

    private TimeUnit periodUnit = TimeUnit.MILLISECONDS;

    private Object input;

    private Predicate<Object> predicate;

    public ConditionCheckerBuilder every(long period, TimeUnit unit) {
      this.period = period;
      periodUnit = unit;
      return this;
    }

    public ConditionCheckerBuilder every(long periodMillis) {
      period = periodMillis;
      periodUnit = TimeUnit.MILLISECONDS;
      return this;
    }

    public ConditionCheckerBuilder before(long timeout, TimeUnit unit) {
      this.timeout = timeout;
      timeoutUnit = unit;
      return this;
    }

    public ConditionCheckerBuilder before(long timeoutMillis) {
      timeout = timeoutMillis;
      timeoutUnit = TimeUnit.MILLISECONDS;
      return this;
    }

    @SuppressWarnings("unchecked")
    public <T> ConditionCheckerBuilder that(T input, Predicate<T> predicate) {
      this.input = input;
      this.predicate = (Predicate<Object>) predicate;
      return this;
    }

    public ConditionCheckerBuilder that(final Callable<Boolean> condition) {
      this.input = null;
      this.predicate =
          input -> {
            try {
              return condition.call();
            } catch (Exception e) {
              logger.error("Evaluation of condition threw exception", e);
              return false;
            }
          };
      return this;
    }

    @SuppressWarnings("unchecked")
    public void becomesTrue() {
      new ConditionChecker(input, predicate, period, periodUnit).await(timeout, timeoutUnit);
    }

    @SuppressWarnings("unchecked")
    public void becomesFalse() {
      this.predicate = predicate.negate();
      new ConditionChecker(input, predicate, period, periodUnit).await(timeout, timeoutUnit);
    }
  }
}
