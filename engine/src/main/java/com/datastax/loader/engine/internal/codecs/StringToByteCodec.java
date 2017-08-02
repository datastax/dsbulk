/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import java.math.BigDecimal;
import java.text.DecimalFormat;

public class StringToByteCodec extends StringToNumberCodec<Byte> {

  public StringToByteCodec(ThreadLocal<DecimalFormat> formatter) {
    super(tinyInt(), formatter);
  }

  @Override
  protected Byte convertFrom(String s) {
    BigDecimal number = parseAsBigDecimal(s);
    if (number == null) {
      return null;
    }
    return number.byteValueExact();
  }
}
