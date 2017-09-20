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

public class StringToBigDecimalCodec extends StringToNumberCodec<BigDecimal> {

  public StringToBigDecimalCodec(ThreadLocal<DecimalFormat> formatter) {
    super(TypeCodec.decimal(), formatter);
  }

  @Override
  public BigDecimal convertFrom(String s) {
    return parseAsBigDecimal(s);
  }
}
