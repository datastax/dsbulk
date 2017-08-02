/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import java.math.BigDecimal;
import java.text.DecimalFormat;

public class StringToBigDecimalCodec extends StringToNumberCodec<BigDecimal> {

  public StringToBigDecimalCodec(ThreadLocal<DecimalFormat> formatter) {
    super(decimal(), formatter);
  }

  @Override
  protected DecimalFormat getNumberFormat() {
    DecimalFormat format = super.getNumberFormat();
    format.setParseBigDecimal(true);
    return format;
  }

  @Override
  protected BigDecimal convertFrom(String s) {
    return parseAsBigDecimal(s);
  }
}
