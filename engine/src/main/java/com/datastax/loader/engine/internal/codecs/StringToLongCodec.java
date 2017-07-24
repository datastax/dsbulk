/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.text.DecimalFormat;

public class StringToLongCodec extends StringToNumberCodec<Long> {

  public StringToLongCodec(ThreadLocal<DecimalFormat> formatter) {
    super(bigint(), formatter);
  }

  @Override
  protected Long convertFrom(String s) {
    Number number = parseAsNumber(s);
    long value = number.longValue();
    if (value != number.doubleValue()) {
      throw new InvalidTypeException(
          "Invalid long format: " + s, new ArithmeticException("long overflow"));
    }
    return parseAsNumber(s).longValue();
  }
}
