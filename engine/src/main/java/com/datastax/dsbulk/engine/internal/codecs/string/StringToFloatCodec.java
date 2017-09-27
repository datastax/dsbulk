/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.TypeCodec;
import java.math.BigDecimal;
import java.text.DecimalFormat;

public class StringToFloatCodec extends StringToNumberCodec<Float> {

  public StringToFloatCodec(ThreadLocal<DecimalFormat> formatter) {
    super(TypeCodec.cfloat(), formatter);
  }

  @Override
  public Float convertFrom(String s) {
    BigDecimal number = parseAsBigDecimal(s);
    if (number == null) {
      return null;
    }
    return number.floatValue();
  }
}
