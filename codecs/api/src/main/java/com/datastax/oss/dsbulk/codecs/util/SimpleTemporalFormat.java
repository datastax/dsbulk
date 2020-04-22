/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs.util;

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
