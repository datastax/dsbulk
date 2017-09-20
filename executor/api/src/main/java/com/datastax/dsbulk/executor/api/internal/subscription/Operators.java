/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.internal.subscription;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

final class Operators {

  /**
   * Evaluate if a request is strictly positive otherwise throw an exception.
   *
   * @param n the request value
   * @return true if valid
   */
  static boolean validate(long n) {
    if (n == 0) {
      return false;
    }
    if (n < 0) {
      throw new IllegalArgumentException(
          "Spec. Rule 3.9 - Cannot request a non strictly positive number: " + n);
    }
    return true;
  }

  /**
   * Concurrent addition bound to Long.MAX_VALUE. Any concurrent write will "happen" before this
   * operation.
   *
   * @param <T> the parent instance type
   * @param updater current field updater
   * @param instance current instance to update
   * @param toAdd delta to add
   */
  static <T> void addCap(AtomicLongFieldUpdater<T> updater, T instance, long toAdd) {
    long r, u;
    do {
      r = updater.get(instance);
      if (r == Long.MAX_VALUE) {
        return;
      }
      u = addCap(r, toAdd);
    } while (!updater.compareAndSet(instance, r, u));
  }

  /**
   * Concurrent subtraction bound to 0. Any concurrent write will "happen" before this operation.
   *
   * @param <T> the parent instance type
   * @param updater current field updater
   * @param instance current instance to update
   * @param toSub delta to sub
   */
  static <T> void produced(AtomicLongFieldUpdater<T> updater, T instance, long toSub) {
    long r, u;
    do {
      r = updater.get(instance);
      if (r == 0 || r == Long.MAX_VALUE) {
        return;
      }
      u = subCap(r, toSub);
    } while (!updater.compareAndSet(instance, r, u));
  }

  /**
   * Cap an addition to Long.MAX_VALUE
   *
   * @param a left operand
   * @param b right operand
   * @return Addition result or Long.MAX_VALUE if overflow
   */
  private static long addCap(long a, long b) {
    long res = a + b;
    if (res < 0L) {
      return Long.MAX_VALUE;
    }
    return res;
  }

  /**
   * Cap a subtraction to 0
   *
   * @param a left operand
   * @param b right operand
   * @return Subscription result or 0 if overflow
   */
  private static long subCap(long a, long b) {
    long res = a - b;
    if (res < 0L) {
      return 0;
    }
    return res;
  }
}
