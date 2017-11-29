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
  public FROM convertTo(TO value) {
    return (FROM) convert(value, getJavaType());
  }

  @SuppressWarnings("unchecked")
  @Override
  public TO convertFrom(FROM value) {
    return (TO) convert(value, targetCodec.getJavaType());
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
    Date date = null;
    Class<?> rawType = targetType.getRawType();
    if (value instanceof LocalDate) {
      date = Date.from(((LocalDate) value).atStartOfDay(timeZone).toInstant());
    } else if (value instanceof LocalTime) {
      date =
          Date.from(
              ((LocalTime) value).atDate(LocalDate.ofEpochDay(0)).atZone(timeZone).toInstant());
    } else if (value instanceof LocalDateTime) {
      date = Date.from(((LocalDateTime) value).atZone(timeZone).toInstant());
    } else if (value instanceof Instant) {
      date = Date.from(((Instant) value));
    } else if (value instanceof ZonedDateTime) {
      date = Date.from(((ZonedDateTime) value).toInstant());
    }
    if (date == null) {
      throw new InvalidTypeException(
          String.format("Cannot convert %s of type %s to %s", value, value.getClass(), rawType));
    }
    if (rawType.equals(java.sql.Date.class)) {
      return new java.sql.Date(date.getTime());
    }
    return date;
  }
}
