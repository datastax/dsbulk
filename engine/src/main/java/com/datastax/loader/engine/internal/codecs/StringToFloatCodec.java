/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.text.DecimalFormat;

public class StringToFloatCodec extends StringToNumberCodec<Float> {

  public StringToFloatCodec(ThreadLocal<DecimalFormat> formatter) {
    super(cfloat(), formatter);
  }

  @Override
  protected Float convertFrom(String s) {
    Number number = parseAsNumber(s);
    float value = number.floatValue();
    if (value != number.doubleValue()) {
      throw new InvalidTypeException(
          "Invalid float format: " + s, new ArithmeticException("float overflow"));
    }
    return value;
  }
}
