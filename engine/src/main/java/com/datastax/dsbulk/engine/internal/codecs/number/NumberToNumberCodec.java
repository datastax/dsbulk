/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.number;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
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
  public FROM convertTo(TO value) {
    return (FROM) convert(value, getJavaType());
  }

  @SuppressWarnings("unchecked")
  @Override
  public TO convertFrom(FROM value) {
    return (TO) convert(value, targetCodec.getJavaType());
  }

  private Number convert(Number value, TypeToken<? extends Number> targetType) {
    if (value == null) {
      return null;
    }
    Class<?> targetClass = targetType.getRawType();
    try {
      if (targetClass.equals(Byte.class)) {
        return CodecUtils.toByteValueExact(value);
      }
      if (targetClass.equals(Short.class)) {
        return CodecUtils.toShortValueExact(value);
      }
      if (targetClass.equals(Integer.class)) {
        return CodecUtils.toIntValueExact(value);
      }
      if (targetClass.equals(Long.class)) {
        return CodecUtils.toLongValueExact(value);
      }
      if (targetClass.equals(Float.class)) {
        return CodecUtils.toFloatValue(value);
      }
      if (targetClass.equals(Double.class)) {
        return CodecUtils.toDoubleValue(value);
      }
      if (targetClass.equals(BigInteger.class)) {
        return CodecUtils.toBigIntegerExact(value);
      }
      if (targetClass.equals(BigDecimal.class)) {
        return CodecUtils.toBigDecimal(value);
      }
    } catch (ArithmeticException e) {
      throw new InvalidTypeException(
          String.format("Cannot convert %s of type %s to %s", value, value.getClass(), targetClass),
          e);
    }
    throw new InvalidTypeException(
        String.format("Cannot convert %s of type %s to %s", value, value.getClass(), targetClass));
  }
}
