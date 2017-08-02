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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;

public class TemporalToTemporalCodec<FROM extends TemporalAccessor, TO extends TemporalAccessor>
    extends ConvertingCodec<FROM, TO> {

  public TemporalToTemporalCodec(Class<FROM> javaType, TypeCodec<TO> targetCodec) {
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
        return ((Instant) value).atZone(ZoneOffset.UTC).toLocalDate();
      }
      if (value instanceof ZonedDateTime) {
        return ((ZonedDateTime) value).toLocalDate();
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
        return ((Instant) value).atZone(ZoneOffset.UTC).toLocalTime();
      }
      if (value instanceof ZonedDateTime) {
        return ((ZonedDateTime) value).toLocalTime();
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
        return ((Instant) value).atZone(ZoneOffset.UTC).toLocalDateTime();
      }
      if (value instanceof ZonedDateTime) {
        return ((ZonedDateTime) value).toLocalDateTime();
      }
    }
    if (rawType.equals(Instant.class)) {
      if (value instanceof LocalDate) {
        return ((LocalDate) value).atStartOfDay(ZoneOffset.UTC).toInstant();
      }
      if (value instanceof LocalTime) {
        return ((LocalTime) value)
            .atDate(LocalDate.ofEpochDay(0))
            .atZone(ZoneOffset.UTC)
            .toInstant();
      }
      if (value instanceof LocalDateTime) {
        return ((LocalDateTime) value).atZone(ZoneOffset.UTC).toInstant();
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
        return ((LocalDate) value).atStartOfDay(ZoneOffset.UTC);
      }
      if (value instanceof LocalTime) {
        return ((LocalTime) value).atDate(LocalDate.ofEpochDay(0)).atZone(ZoneOffset.UTC);
      }
      if (value instanceof LocalDateTime) {
        return ((LocalDateTime) value).atZone(ZoneOffset.UTC);
      }
      if (value instanceof Instant) {
        return ((Instant) value).atZone(ZoneOffset.UTC);
      }
      if (value instanceof ZonedDateTime) {
        return value;
      }
    }
    throw new InvalidTypeException(
        String.format("Cannot convert %s of type %s to %s", value, value.getClass(), rawType));
  }
}
