/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.text.DecimalFormat;

public class StringToIntegerCodec extends StringToNumberCodec<Integer> {

  public StringToIntegerCodec(ThreadLocal<DecimalFormat> formatter) {
    super(cint(), formatter);
  }

  @Override
  protected Integer convertFrom(String s) {
    Number number = parseAsNumber(s);
    int value = number.intValue();
    if (value != number.doubleValue()) {
      throw new InvalidTypeException(
          "Invalid integer format: " + s, new ArithmeticException("integer overflow"));
    }
    return value;
  }
}
