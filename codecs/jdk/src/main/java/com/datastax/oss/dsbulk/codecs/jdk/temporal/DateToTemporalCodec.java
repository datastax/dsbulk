/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.codecs.jdk.temporal;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import java.sql.Time;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.Date;

public class DateToTemporalCodec<EXTERNAL extends Date, INTERNAL extends TemporalAccessor>
    extends ConvertingCodec<EXTERNAL, INTERNAL> {

  private final ZoneId timeZone;

  public DateToTemporalCodec(
      Class<EXTERNAL> javaType, TypeCodec<INTERNAL> targetCodec, ZoneId timeZone) {
    super(targetCodec, javaType);
    this.timeZone = timeZone;
  }

  @SuppressWarnings("unchecked")
  @Override
  public EXTERNAL internalToExternal(INTERNAL value) {
    return (EXTERNAL) convert(value, getJavaType());
  }

  @SuppressWarnings("unchecked")
  @Override
  public INTERNAL externalToInternal(EXTERNAL value) {
    return (INTERNAL) convert(value, internalCodec.getJavaType());
  }

  private TemporalAccessor convert(Date value, GenericType<? extends TemporalAccessor> targetType) {
    if (value == null) {
      return null;
    }
    if (targetType.equals(GenericType.LOCAL_DATE)) {
      if (value instanceof java.sql.Date) {
        return ((java.sql.Date) value).toLocalDate();
      } else {
        return value.toInstant().atZone(timeZone).toLocalDate();
      }
    }
    if (targetType.equals(GenericType.LOCAL_TIME)) {
      if (value instanceof Time) {
        return ((Time) value).toLocalTime();
      } else {
        return value.toInstant().atZone(timeZone).toLocalTime();
      }
    }
    if (targetType.equals(GenericType.of(LocalDateTime.class))) {
      if (value instanceof java.sql.Date) {
        return ((java.sql.Date) value).toLocalDate().atStartOfDay();
      } else if (value instanceof Time) {
        return ((Time) value).toLocalTime().atDate(LocalDate.ofEpochDay(0));
      } else {
        return value.toInstant().atZone(timeZone).toLocalDateTime();
      }
    }
    if (targetType.equals(GenericType.INSTANT)) {
      if (value instanceof java.sql.Date) {
        return ((java.sql.Date) value).toLocalDate().atStartOfDay(timeZone).toInstant();
      } else if (value instanceof Time) {
        return ((Time) value)
            .toLocalTime()
            .atDate(LocalDate.ofEpochDay(0))
            .atZone(timeZone)
            .toInstant();
      } else {
        return value.toInstant();
      }
    }
    if (targetType.equals(GenericType.of(ZonedDateTime.class))) {
      if (value instanceof java.sql.Date) {
        return ((java.sql.Date) value).toLocalDate().atStartOfDay(timeZone);
      } else if (value instanceof Time) {
        return ((Time) value).toLocalTime().atDate(LocalDate.ofEpochDay(0)).atZone(timeZone);
      } else {
        return value.toInstant().atZone(timeZone);
      }
    }
    throw new IllegalArgumentException(
        String.format("Cannot convert %s of type %s to %s", value, value.getClass(), targetType));
  }

  private Date convert(TemporalAccessor value, GenericType<? extends Date> targetType) {
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
    } else if (rawType.equals(Time.class)) {
      if (value instanceof LocalTime) {
        return Time.valueOf(((LocalTime) value));
      } else if (value instanceof LocalDateTime) {
        return Time.valueOf(((LocalDateTime) value).toLocalTime());
      } else if (value instanceof Instant) {
        return Time.valueOf(((Instant) value).atZone(timeZone).toLocalTime());
      } else if (value instanceof ZonedDateTime) {
        return Time.valueOf(((ZonedDateTime) value).withZoneSameInstant(timeZone).toLocalTime());
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
    } else if (rawType.equals(Date.class)) {
      if (value instanceof LocalDate) {
        return Date.from(((LocalDate) value).atStartOfDay(timeZone).toInstant());
      } else if (value instanceof LocalTime) {
        return Date.from(
            ((LocalTime) value).atDate(LocalDate.ofEpochDay(0)).atZone(timeZone).toInstant());
      } else if (value instanceof LocalDateTime) {
        return Date.from(((LocalDateTime) value).atZone(timeZone).toInstant());
      } else if (value instanceof Instant) {
        return Date.from((Instant) value);
      } else if (value instanceof ZonedDateTime) {
        return Date.from(((ZonedDateTime) value).toInstant());
      }
    }
    throw new IllegalArgumentException(
        String.format("Cannot convert %s of type %s to %s", value, value.getClass(), rawType));
  }
}
