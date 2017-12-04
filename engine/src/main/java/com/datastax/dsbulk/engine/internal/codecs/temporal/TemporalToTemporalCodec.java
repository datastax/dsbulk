/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.temporal;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.google.common.reflect.TypeToken;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;

public class TemporalToTemporalCodec<FROM extends TemporalAccessor, TO extends TemporalAccessor>
    extends ConvertingCodec<FROM, TO> {

  private final ZoneId timeZone;

  public TemporalToTemporalCodec(Class<FROM> javaType, TypeCodec<TO> targetCodec, ZoneId timeZone) {
    super(targetCodec, javaType);
    this.timeZone = timeZone;
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

  private TemporalAccessor convert(
      TemporalAccessor value, TypeToken<? extends TemporalAccessor> targetType) {
    if (value == null) {
      return null;
    }
    Class<?> rawType = targetType.getRawType();
    if (rawType.equals(LocalDate.class)) {
      if (value instanceof LocalDate) {
        return value;
      }
      if (value instanceof LocalDateTime) {
        return ((LocalDateTime) value).toLocalDate();
      }
      if (value instanceof Instant) {
        return ((Instant) value).atZone(timeZone).toLocalDate();
      }
      if (value instanceof ZonedDateTime) {
        return ((ZonedDateTime) value).withZoneSameInstant(timeZone).toLocalDate();
      }
    }
    if (rawType.equals(LocalTime.class)) {
      if (value instanceof LocalTime) {
        return value;
      }
      if (value instanceof LocalDateTime) {
        return ((LocalDateTime) value).toLocalTime();
      }
      if (value instanceof Instant) {
        return ((Instant) value).atZone(timeZone).toLocalTime();
      }
      if (value instanceof ZonedDateTime) {
        return ((ZonedDateTime) value).withZoneSameInstant(timeZone).toLocalTime();
      }
    }
    if (rawType.equals(LocalDateTime.class)) {
      if (value instanceof LocalDate) {
        return ((LocalDate) value).atStartOfDay();
      }
      if (value instanceof LocalTime) {
        return ((LocalTime) value).atDate(LocalDate.ofEpochDay(0));
      }
      if (value instanceof LocalDateTime) {
        return value;
      }
      if (value instanceof Instant) {
        return ((Instant) value).atZone(timeZone).toLocalDateTime();
      }
      if (value instanceof ZonedDateTime) {
        return ((ZonedDateTime) value).withZoneSameInstant(timeZone).toLocalDateTime();
      }
    }
    if (rawType.equals(Instant.class)) {
      if (value instanceof LocalDate) {
        return ((LocalDate) value).atStartOfDay(timeZone).toInstant();
      }
      if (value instanceof LocalTime) {
        return ((LocalTime) value).atDate(LocalDate.ofEpochDay(0)).atZone(timeZone).toInstant();
      }
      if (value instanceof LocalDateTime) {
        return ((LocalDateTime) value).atZone(timeZone).toInstant();
      }
      if (value instanceof Instant) {
        return value;
      }
      if (value instanceof ZonedDateTime) {
        return ((ZonedDateTime) value).toInstant();
      }
    }
    if (rawType.equals(ZonedDateTime.class)) {
      if (value instanceof LocalDate) {
        return ((LocalDate) value).atStartOfDay(timeZone);
      }
      if (value instanceof LocalTime) {
        return ((LocalTime) value).atDate(LocalDate.ofEpochDay(0)).atZone(timeZone);
      }
      if (value instanceof LocalDateTime) {
        return ((LocalDateTime) value).atZone(timeZone);
      }
      if (value instanceof Instant) {
        return ((Instant) value).atZone(timeZone);
      }
      if (value instanceof ZonedDateTime) {
        return value;
      }
    }
    throw new InvalidTypeException(
        String.format("Cannot convert %s of type %s to %s", value, value.getClass(), rawType));
  }
}
