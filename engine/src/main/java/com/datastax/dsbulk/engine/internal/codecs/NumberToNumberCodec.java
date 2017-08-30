/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs;

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
    if (value == null) {
      return null;
    }
    Class<?> targetClass = targetType.getRawType();
    try {
      if (targetClass.equals(Byte.class)) {
        return toByteValueExact(value);
      }
      if (targetClass.equals(Short.class)) {
        return toShortValueExact(value);
      }
      if (targetClass.equals(Integer.class)) {
        return toIntValueExact(value);
      }
      if (targetClass.equals(Long.class)) {
        return toLongValueExact(value);
      }
      if (targetClass.equals(Float.class)) {
        return toFloatValue(value);
      }
      if (targetClass.equals(Double.class)) {
        return toDoubleValue(value);
      }
      if (targetClass.equals(BigInteger.class)) {
        return toBigIntegerExact(value);
      }
      if (targetClass.equals(BigDecimal.class)) {
        return toBigDecimal(value);
      }
    } catch (ArithmeticException e) {
      throw new InvalidTypeException(
          String.format("Cannot convert %s of type %s to %s", value, value.getClass(), targetClass),
          e);
    }
    throw new InvalidTypeException(
        String.format("Cannot convert %s of type %s to %s", value, value.getClass(), targetClass));
  }

  private byte toByteValueExact(Number value) {
    if (value instanceof Byte) {
      return (byte) value;
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).byteValueExact();
    } else {
      return new BigDecimal(value.toString()).byteValueExact();
    }
  }

  private short toShortValueExact(Number value) {
    if (value instanceof Short) {
      return (short) value;
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).shortValueExact();
    } else {
      return new BigDecimal(value.toString()).shortValueExact();
    }
  }

  private int toIntValueExact(Number value) {
    if (value instanceof Integer) {
      return (int) value;
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).intValueExact();
    } else {
      return new BigDecimal(value.toString()).intValueExact();
    }
  }

  private long toLongValueExact(Number value) {
    if (value instanceof Long) {
      return (long) value;
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).longValueExact();
    } else {
      return new BigDecimal(value.toString()).longValueExact();
    }
  }

  private Number toFloatValue(Number value) {
    if (value instanceof Float) {
      return value;
    } else {
      return value.floatValue();
    }
  }

  private Number toDoubleValue(Number value) {
    if (value instanceof Double) {
      return value;
    } else {
      return value.doubleValue();
    }
  }

  private BigInteger toBigIntegerExact(Number value) {
    if (value instanceof BigInteger) {
      return (BigInteger) value;
    } else if (value instanceof BigDecimal) {
      return ((BigDecimal) value).toBigIntegerExact();
    } else {
      return new BigDecimal(value.toString()).toBigIntegerExact();
    }
  }

  private Number toBigDecimal(Number value) {
    if (value instanceof BigDecimal) {
      return value;
    } else {
      return new BigDecimal(value.toString());
    }
  }
}
