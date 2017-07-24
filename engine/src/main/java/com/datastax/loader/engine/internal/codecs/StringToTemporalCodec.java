/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.text.ParseException;
import java.text.ParsePosition;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;

public abstract class StringToTemporalCodec<T extends TemporalAccessor>
    extends ConvertingCodec<String, T> {

  protected final DateTimeFormatter parser;

  public StringToTemporalCodec(TypeCodec<T> targetCodec, DateTimeFormatter parser) {
    super(targetCodec, String.class);
    this.parser = parser;
  }

  protected TemporalAccessor parseAsTemporalAccessor(String s) {
    if (s == null || s.isEmpty()) return null;
    try {
      ParsePosition pos = new ParsePosition(0);
      TemporalAccessor accessor = parser.parse(s, pos);
      if (pos.getIndex() != s.length())
        throw new InvalidTypeException(
            "Cannot parse temporal: " + s, new ParseException(s, pos.getErrorIndex()));
      return accessor;
    } catch (DateTimeParseException e) {
      throw new InvalidTypeException("Cannot parse temporal: " + s, e);
    }
  }

  @Override
  protected String convertTo(T value) {
    if (value == null) return null;
    return parser.format(value);
  }
}
