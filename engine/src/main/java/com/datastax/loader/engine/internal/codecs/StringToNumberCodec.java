/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.ParsePosition;

public abstract class StringToNumberCodec<N extends Number> extends ConvertingCodec<String, N> {

  private final ThreadLocal<DecimalFormat> formatter;

  public StringToNumberCodec(TypeCodec<N> targetCodec, ThreadLocal<DecimalFormat> formatter) {
    super(targetCodec, String.class);
    this.formatter = formatter;
  }

  protected Number parseAsNumber(String s) {
    if (s == null || s.isEmpty()) return null;
    ParsePosition pos = new ParsePosition(0);
    Number number = getNumberFormat().parse(s.trim(), pos);
    if (number == null)
      throw new InvalidTypeException(
          "Invalid number format: " + s, new ParseException(s, pos.getErrorIndex()));
    if (pos.getIndex() != s.length())
      throw new InvalidTypeException(
          "Invalid number format: " + s, new ParseException(s, pos.getIndex()));
    return number;
  }

  protected DecimalFormat getNumberFormat() {
    return formatter.get();
  }

  @Override
  protected String convertTo(N value) {
    if (value == null) return null;
    return getNumberFormat().format(value);
  }
}
