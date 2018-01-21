/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.internal.subscription;

import java.util.concurrent.atomic.AtomicLong;

final class Operators {

  /**
   * Atomically adds the given value to the given AtomicLong, bound to Long.MAX_VALUE.
   *
   * @param current the current value.
   * @param toAdd the delta to add.
   */
  static void addCap(AtomicLong current, long toAdd) {
    long r, u;
    do {
      r = current.get();
      if (r == Long.MAX_VALUE) {
        return;
      }
      u = addCap(r, toAdd);
    } while (!current.compareAndSet(r, u));
  }

  /**
   * Atomically subtracts the given value from the given AtomicLong, bound to 0.
   *
   * @param current the current value.
   * @param toSub the delta to subtract.
   */
  static void subCap(AtomicLong current, long toSub) {
    long r, u;
    do {
      r = current.get();
      if (r == 0 || r == Long.MAX_VALUE) {
        return;
      }
      u = subCap(r, toSub);
    } while (!current.compareAndSet(r, u));
  }

  /**
   * Adds two long values and caps the sum at Long.MAX_VALUE.
   *
   * @param a the first value
   * @param b the second value
   * @return the sum capped at Long.MAX_VALUE
   */
  private static long addCap(long a, long b) {
    long u = a + b;
    if (u < 0L) {
      return Long.MAX_VALUE;
    }
    return u;
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
