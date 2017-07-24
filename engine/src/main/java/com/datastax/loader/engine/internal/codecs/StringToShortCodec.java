/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.text.DecimalFormat;

public class StringToShortCodec extends StringToNumberCodec<Short> {

  public StringToShortCodec(ThreadLocal<DecimalFormat> formatter) {
    super(smallInt(), formatter);
  }

  @Override
  protected Short convertFrom(String s) {
    Number number = parseAsNumber(s);
    short value = number.shortValue();
    if (value != number.doubleValue()) {
      throw new InvalidTypeException(
          "Invalid short format: " + s, new ArithmeticException("short overflow"));
    }
    return value;
  }
}
