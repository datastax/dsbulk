/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.reflect.TypeToken;
import java.math.BigDecimal;
import java.math.BigInteger;

public class NumberToNumberCodec<FROM extends Number, TO extends Number>
    extends ConvertingCodec<FROM, TO> {

  public NumberToNumberCodec(Class<FROM> javaType, TypeCodec<TO> targetCodec) {
    super(targetCodec, javaType);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected FROM convertTo(TO value) {
    return (FROM) convert(value, getJavaType());
  }

  @SuppressWarnings("unchecked")
  @Override
  protected TO convertFrom(FROM value) {
    return (TO) convert(value, targetCodec.getJavaType());
  }

  private Number convert(Number value, TypeToken<? extends Number> targetType) {
    if (value == null) return null;
    Class<?> rawType = targetType.getRawType();
    if (rawType.equals(Byte.class)) {
      if (value instanceof Byte) return value;
      if (value.byteValue() != value.doubleValue()) {
        throw new InvalidTypeException(
            "Cannot convert to byte: " + value, new ArithmeticException("byte overflow"));
      }
      return value.byteValue();
    }
    if (rawType.equals(Short.class)) {
      if (value instanceof Short) return value;
      if (value.shortValue() != value.doubleValue()) {
        throw new InvalidTypeException(
            "Cannot convert to short: " + value, new ArithmeticException("short overflow"));
      }
      return value.shortValue();
    }
    if (rawType.equals(Integer.class)) {
      if (value instanceof Integer) return value;
      if (value.intValue() != value.doubleValue()) {
        throw new InvalidTypeException(
            "Cannot convert to integer: " + value, new ArithmeticException("integer overflow"));
      }
      return value.intValue();
    }
    if (rawType.equals(Long.class)) {
      if (value instanceof Long) return value;
      if (value.longValue() != value.doubleValue()) {
        throw new InvalidTypeException(
            "Cannot convert to long: " + value, new ArithmeticException("long overflow"));
      }
      return value.longValue();
    }
    if (rawType.equals(Float.class)) {
      if (value instanceof Float) return value;
      if (value.floatValue() != value.doubleValue()) {
        throw new InvalidTypeException(
            "Cannot convert to float: " + value, new ArithmeticException("float overflow"));
      }
      return value.floatValue();
    }
    if (rawType.equals(Double.class)) {
      if (value instanceof Double) return value;
      else return value.doubleValue();
    }
    if (rawType.equals(BigInteger.class)) {
      if (value instanceof BigInteger) return value;
      else return new BigInteger(value.toString());
    }
    if (rawType.equals(BigDecimal.class)) {
      if (value instanceof BigDecimal) return value;
      else return new BigDecimal(value.toString());
    }
    throw new InvalidTypeException(
        String.format("Cannot convert %s of type %s to %s", value, value.getClass(), rawType));
  }
}
