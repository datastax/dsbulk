/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.temporal;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.google.common.reflect.TypeToken;
import java.sql.Time;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.Date;

public class DateToTemporalCodec<FROM extends Date, TO extends TemporalAccessor>
    extends ConvertingCodec<FROM, TO> {

  private final ZoneId timeZone;

  public DateToTemporalCodec(Class<FROM> javaType, TypeCodec<TO> targetCodec, ZoneId timeZone) {
    super(targetCodec, javaType);
    this.timeZone = timeZone;
  }

  @SuppressWarnings("unchecked")
  @Override
  public FROM internalToExternal(TO value) {
    return (FROM) convert(value, getJavaType());
  }

  @SuppressWarnings("unchecked")
  @Override
  public TO externalToInternal(FROM value) {
    return (TO) convert(value, internalCodec.getJavaType());
  }

  private TemporalAccessor convert(Date value, TypeToken<? extends TemporalAccessor> targetType) {
    if (value == null) {
      return null;
    }
    Class<?> rawType = targetType.getRawType();
    if (rawType.equals(LocalDate.class)) {
      if (value instanceof java.sql.Date) {
        return ((java.sql.Date) value).toLocalDate();
      } else {
        return value.toInstant().atZone(timeZone).toLocalDate();
      }
    }
    if (rawType.equals(LocalTime.class)) {
      if (value instanceof java.sql.Time) {
        return ((Time) value).toLocalTime();
      } else {
        return value.toInstant().atZone(timeZone).toLocalTime();
      }
    }
    if (rawType.equals(LocalDateTime.class)) {
      if (value instanceof java.sql.Date) {
        return ((java.sql.Date) value).toLocalDate().atStartOfDay();
      } else if (value instanceof java.sql.Time) {
        return ((Time) value).toLocalTime().atDate(LocalDate.ofEpochDay(0));
      } else {
        return value.toInstant().atZone(timeZone).toLocalDateTime();
      }
    }
    if (rawType.equals(Instant.class)) {
      if (value instanceof java.sql.Date) {
        return ((java.sql.Date) value).toLocalDate().atStartOfDay(timeZone).toInstant();
      } else if (value instanceof java.sql.Time) {
        return ((Time) value)
            .toLocalTime()
            .atDate(LocalDate.ofEpochDay(0))
            .atZone(timeZone)
            .toInstant();
      } else {
        return value.toInstant();
      }
    }
    if (rawType.equals(ZonedDateTime.class)) {
      if (value instanceof java.sql.Date) {
        return ((java.sql.Date) value).toLocalDate().atStartOfDay(timeZone);
      } else if (value instanceof java.sql.Time) {
        return ((Time) value).toLocalTime().atDate(LocalDate.ofEpochDay(0)).atZone(timeZone);
      } else {
        return value.toInstant().atZone(timeZone);
      }
    }
    throw new InvalidTypeException(
        String.format("Cannot convert %s of type %s to %s", value, value.getClass(), rawType));
  }

  private Date convert(TemporalAccessor value, TypeToken<? extends Date> targetType) {
    if (value == null) {
      return null;
    }
    Class<?> rawType = targetType.getRawType();
    if (rawType.equals(java.sql.Date.class)) {
      if (value instanceof LocalDate) {
        return java.sql.Date.valueOf(((LocalDate) value));
      } else if (value instanceof LocalDateTime) {
        return java.sql.Date.valueOf(((LocalDateTime) value).toLocalDate());
      } else if (value instanceof Instant) {
        return java.sql.Date.valueOf(((Instant) value).atZone(timeZone).toLocalDate());
      } else if (value instanceof ZonedDateTime) {
        return java.sql.Date.valueOf(
            ((ZonedDateTime) value).withZoneSameInstant(timeZone).toLocalDate());
      }
    } else if (rawType.equals(java.sql.Time.class)) {
      if (value instanceof LocalTime) {
        return java.sql.Time.valueOf(((LocalTime) value));
      } else if (value instanceof LocalDateTime) {
        return java.sql.Time.valueOf(((LocalDateTime) value).toLocalTime());
      } else if (value instanceof Instant) {
        return java.sql.Time.valueOf(((Instant) value).atZone(timeZone).toLocalTime());
      } else if (value instanceof ZonedDateTime) {
        return java.sql.Time.valueOf(
            ((ZonedDateTime) value).withZoneSameInstant(timeZone).toLocalTime());
      }
    } else if (rawType.equals(java.sql.Timestamp.class)) {
      if (value instanceof LocalDate) {
        return java.sql.Timestamp.from(((LocalDate) value).atStartOfDay(timeZone).toInstant());
      } else if (value instanceof LocalTime) {
        return java.sql.Timestamp.from(
            ((LocalTime) value).atDate(LocalDate.ofEpochDay(0)).atZone(timeZone).toInstant());
      } else if (value instanceof LocalDateTime) {
        return java.sql.Timestamp.from(((LocalDateTime) value).atZone(timeZone).toInstant());
      } else if (value instanceof Instant) {
        return java.sql.Timestamp.from((Instant) value);
      } else if (value instanceof ZonedDateTime) {
        return java.sql.Timestamp.from(((ZonedDateTime) value).toInstant());
      }
    } else if (rawType.equals(java.util.Date.class)) {
      if (value instanceof LocalDate) {
        return java.util.Date.from(((LocalDate) value).atStartOfDay(timeZone).toInstant());
      } else if (value instanceof LocalTime) {
        return java.util.Date.from(
            ((LocalTime) value).atDate(LocalDate.ofEpochDay(0)).atZone(timeZone).toInstant());
      } else if (value instanceof LocalDateTime) {
        return java.util.Date.from(((LocalDateTime) value).atZone(timeZone).toInstant());
      } else if (value instanceof Instant) {
        return java.util.Date.from((Instant) value);
      } else if (value instanceof ZonedDateTime) {
        return java.util.Date.from(((ZonedDateTime) value).toInstant());
      }
    }
    throw new InvalidTypeException(
        String.format("Cannot convert %s of type %s to %s", value, value.getClass(), rawType));
  }
}
