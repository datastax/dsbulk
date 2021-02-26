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
package com.datastax.oss.dsbulk.codecs.api.format.temporal;

import com.datastax.oss.dsbulk.codecs.api.util.CodecUtils;
import io.netty.util.concurrent.FastThreadLocal;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.concurrent.TimeUnit;

/** A special temporal format that converts to and from numeric data. */
public class NumericTemporalFormat implements TemporalFormat {

  private final FastThreadLocal<NumberFormat> numberFormat;
  private final ZoneId timeZone;
  private final TimeUnit timeUnit;
  private final ZonedDateTime epoch;

  public NumericTemporalFormat(
      FastThreadLocal<NumberFormat> numberFormat,
      ZoneId timeZone,
      TimeUnit timeUnit,
      ZonedDateTime epoch) {
    this.timeUnit = timeUnit;
    this.epoch = epoch;
    this.numberFormat = numberFormat;
    this.timeZone = timeZone;
  }

  @Override
  public TemporalAccessor parse(String text) throws DateTimeException {
    if (text == null || text.isEmpty()) {
      return null;
    }
    ParsePosition pos = new ParsePosition(0);
    Number n = numberFormat.get().parse(text, pos);
    if (pos.getIndex() == 0) {
      // input could not be parsed at all
      throw new DateTimeParseException(
          String.format("Could not parse temporal at index %d: %s", pos.getErrorIndex(), text),
          text,
          pos.getErrorIndex());
    } else if (pos.getIndex() != text.length()) {
      // input has been partially parsed
      throw new DateTimeParseException(
          String.format("Could not parse temporal at index %d: %s", pos.getIndex(), text),
          text,
          pos.getIndex());
    }
    return numberToTemporal(n);
  }

  @Override
  public String format(TemporalAccessor temporal) throws DateTimeException {
    Number n = temporalToNumber(temporal);
    if (n == null) {
      return null;
    }
    try {
      return numberFormat.get().format(n);
    } catch (Exception e) {
      throw new DateTimeException("Could not format temporal: " + temporal, e);
    }
  }

  public TemporalAccessor numberToTemporal(Number n) {
    if (n == null) {
      return null;
    }
    try {
      return CodecUtils.numberToInstant(n, timeUnit, epoch.toInstant());
    } catch (Exception e) {
      throw new DateTimeException("Could not convert number to temporal: " + n, e);
    }
  }

  public Number temporalToNumber(TemporalAccessor temporal) throws DateTimeException {
    if (temporal == null) {
      return null;
    }
    try {
      Instant i = CodecUtils.toInstant(temporal, timeZone, epoch.toLocalDate());
      return CodecUtils.instantToNumber(i, timeUnit, epoch.toInstant());
    } catch (Exception e) {
      throw new DateTimeException("Could not convert temporal to number: " + temporal, e);
    }
  }
}
