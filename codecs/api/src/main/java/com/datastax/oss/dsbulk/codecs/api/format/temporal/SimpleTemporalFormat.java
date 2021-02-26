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

import java.text.ParsePosition;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Objects;

/**
 * A generic temporal format. For zoned temporals, it is often preferable to use {@link
 * ZonedTemporalFormat} instead.
 */
public class SimpleTemporalFormat implements TemporalFormat {

  private final DateTimeFormatter parser;
  private final DateTimeFormatter formatter;

  public SimpleTemporalFormat(DateTimeFormatter parserAndFormatter) {
    this(parserAndFormatter, parserAndFormatter);
  }

  public SimpleTemporalFormat(DateTimeFormatter parser, DateTimeFormatter formatter) {
    this.parser = Objects.requireNonNull(parser, "parser cannot be null");
    this.formatter = Objects.requireNonNull(formatter, "formatter cannot be null");
  }

  @Override
  public TemporalAccessor parse(String text) {
    if (text == null || text.isEmpty()) {
      return null;
    }
    ParsePosition pos = new ParsePosition(0);
    TemporalAccessor temporal = parser.parse(text.trim(), pos);
    if (pos.getIndex() != text.length()) {
      throw new DateTimeParseException(
          String.format("Could not parse temporal at index %d: %s", pos.getIndex(), text),
          text,
          pos.getIndex());
    }
    return temporal;
  }

  @Override
  public String format(TemporalAccessor temporal) {
    if (temporal == null) {
      return null;
    }
    return formatter.format(temporal);
  }
}
