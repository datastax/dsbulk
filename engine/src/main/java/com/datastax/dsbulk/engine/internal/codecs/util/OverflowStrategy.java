/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

public enum OverflowStrategy {
  REJECT {
    @Override
    Number apply(
        Number value,
        ArithmeticException e,
        Class<? extends Number> targetClass,
        RoundingMode roundingMode) {
      throw e;
    }
  },

  TRUNCATE {
    @Override
    Number apply(
        Number value,
        ArithmeticException e,
        Class<? extends Number> targetClass,
        RoundingMode roundingMode) {
      if (targetClass.equals(Byte.class)) {
        // Value too large
        if (value.longValue() > Byte.MAX_VALUE) return Byte.MAX_VALUE;
        if (value.longValue() < Byte.MIN_VALUE) return Byte.MIN_VALUE;
        // Value has decimals, round first before narrowing
        if (value.doubleValue() % 1 != 0) {
          value = CodecUtils.toBigDecimal(value).setScale(0, roundingMode);
        }
        // Let Java type narrowing rules apply
        return value.byteValue();
      }
      if (targetClass.equals(Short.class)) {
        if (value.longValue() > Short.MAX_VALUE) return Short.MAX_VALUE;
        if (value.longValue() < Short.MIN_VALUE) return Short.MIN_VALUE;
        // Value has decimals, round first before narrowing
        if (value.doubleValue() % 1 != 0) {
          value = CodecUtils.toBigDecimal(value).setScale(0, roundingMode);
        }
        // Let Java type narrowing rules apply
        return value.shortValue();
      }
      if (targetClass.equals(Integer.class)) {
        if (value.longValue() > Integer.MAX_VALUE) return Integer.MAX_VALUE;
        if (value.longValue() < Integer.MIN_VALUE) return Integer.MIN_VALUE;
        // Value has decimals, round first before narrowing
        if (value.doubleValue() % 1 != 0) {
          value = CodecUtils.toBigDecimal(value).setScale(0, roundingMode);
        }
        // Let Java type narrowing rules apply
        return value.intValue();
      }
      if (targetClass.equals(Long.class)) {
        BigDecimal d = CodecUtils.toBigDecimal(value);
        if (d.compareTo(LONG_MAX_VALUE) > 0) return Long.MAX_VALUE;
        if (d.compareTo(LONG_MIN_VALUE) < 0) return Long.MIN_VALUE;
        // Value has decimals, round first before narrowing
        if (value.doubleValue() % 1 != 0) {
          value = d.setScale(0, roundingMode);
        }
        // Let Java type narrowing rules apply
        return value.longValue();
      }
      if (targetClass.equals(BigInteger.class)) {
        // Value has decimals, round first before narrowing
        if (value.doubleValue() % 1 != 0) {
          value = CodecUtils.toBigDecimal(value).setScale(0, roundingMode);
        }
        // Let Java type narrowing rules apply
        return CodecUtils.toBigDecimal(value).toBigInteger();
      }
      if (targetClass.equals(Float.class)) {
        if (value.doubleValue() > Float.MAX_VALUE) return Float.MAX_VALUE;
        if (value.doubleValue() < Float.MIN_VALUE) return Float.MIN_VALUE;
        // Let IEEE 754 rounding rules apply
        return value.floatValue();
      }
      if (targetClass.equals(Double.class)) {
        BigDecimal d = CodecUtils.toBigDecimal(value);
        if (d.compareTo(DOUBLE_MAX_VALUE) > 0) return Double.MAX_VALUE;
        if (d.compareTo(DOUBLE_MIN_VALUE) < 0) return Double.MIN_VALUE;
        // Let IEEE 754 rounding rules apply
        return value.doubleValue();
      }
      if (targetClass.equals(BigDecimal.class)) {
        // an overflow here cannot happen
        return value;
      }
      // Unknown target class.
      throw e;
    }
  };

  private static final BigDecimal LONG_MIN_VALUE = BigDecimal.valueOf(Long.MIN_VALUE);
  private static final BigDecimal LONG_MAX_VALUE = BigDecimal.valueOf(Long.MAX_VALUE);
  private static final BigDecimal DOUBLE_MIN_VALUE = BigDecimal.valueOf(Double.MIN_VALUE);
  private static final BigDecimal DOUBLE_MAX_VALUE = BigDecimal.valueOf(Double.MAX_VALUE);

  abstract Number apply(
      Number value,
      ArithmeticException e,
      Class<? extends Number> targetClass,
      RoundingMode roundingMode);
}
