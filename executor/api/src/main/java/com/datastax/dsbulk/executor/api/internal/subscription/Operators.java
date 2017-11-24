/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.internal.subscription;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

final class Operators {

  /**
   * Evaluates if a request is strictly positive otherwise throws an exception.
   *
   * @param n the request value.
   * @return true if > 0, false if == 0.
   * @throws IllegalArgumentException if the request is invalid (i.e., < 0).
   */
  static boolean validate(long n) {
    if (n == 0) {
      return false;
    }
    if (n < 0) {
      throw new IllegalArgumentException("Spec. Rule 3.9 - Cannot request a negative number: " + n);
    }
    return true;
  }

  /**
   * Atomically adds the given value to the given field, bound to Long.MAX_VALUE.
   *
   * @param <T> the field's declaring class.
   * @param updater the field updater instance.
   * @param instance the owning instance to update.
   * @param toAdd the delta to add.
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
   * Atomically subtracts the given value from the given field, bound to 0.
   *
   * @param <T> the field's declaring class.
   * @param updater the field updater instance.
   * @param instance the owning instance to update.
   * @param toSub the delta to subtract.
   */
  static <T> void subCap(AtomicLongFieldUpdater<T> updater, T instance, long toSub) {
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
   * Caps an addition to Long.MAX_VALUE.
   *
   * @param a left operand.
   * @param b right operand.
   * @return Addition result or Long.MAX_VALUE if overflow.
   */
  private static long addCap(long a, long b) {
    long res = a + b;
    if (res < 0L) {
      return Long.MAX_VALUE;
    }
    return res;
  }

  /**
   * Caps a subtraction to 0.
   *
   * @param a left operand.
   * @param b right operand.
   * @return Subscription result or 0 if overflow.
   */
  private static long subCap(long a, long b) {
    long res = a - b;
    if (res < 0L) {
      return 0;
    }
    return res;
  }
}
