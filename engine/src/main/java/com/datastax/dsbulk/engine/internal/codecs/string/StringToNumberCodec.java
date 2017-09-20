/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.ParsePosition;

public abstract class StringToNumberCodec<N extends Number> extends ConvertingCodec<String, N> {

  private final ThreadLocal<DecimalFormat> formatter;

  public StringToNumberCodec(TypeCodec<N> targetCodec, ThreadLocal<DecimalFormat> formatter) {
    super(targetCodec, String.class);
    this.formatter = formatter;
  }

  @Override
  public String convertTo(N value) {
    if (value == null) {
      return null;
    }
    return getNumberFormat().format(value);
  }

  protected BigDecimal parseAsBigDecimal(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    ParsePosition pos = new ParsePosition(0);
    BigDecimal number = (BigDecimal) getNumberFormat().parse(s.trim(), pos);
    if (number == null) {
      throw new InvalidTypeException(
          "Invalid number format: " + s, new ParseException(s, pos.getErrorIndex()));
    }
    if (pos.getIndex() != s.length()) {
      throw new InvalidTypeException(
          "Invalid number format: " + s, new ParseException(s, pos.getIndex()));
    }
    return number;
  }

  private DecimalFormat getNumberFormat() {
    DecimalFormat format = formatter.get();
    format.setParseBigDecimal(true);
    return format;
  }
}
