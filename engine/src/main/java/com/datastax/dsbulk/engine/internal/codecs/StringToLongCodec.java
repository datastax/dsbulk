/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs;

import java.math.BigDecimal;
import java.text.DecimalFormat;

public class StringToLongCodec extends StringToNumberCodec<Long> {

  public StringToLongCodec(ThreadLocal<DecimalFormat> formatter) {
    super(bigint(), formatter);
  }

  @Override
  protected Long convertFrom(String s) {
    BigDecimal number = parseAsBigDecimal(s);
    if (number == null) {
      return null;
    }
    return number.longValueExact();
  }
}
