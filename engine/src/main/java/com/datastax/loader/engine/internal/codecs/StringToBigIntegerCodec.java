/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;

public class StringToBigIntegerCodec extends StringToNumberCodec<BigInteger> {

  public StringToBigIntegerCodec(ThreadLocal<DecimalFormat> formatter) {
    super(varint(), formatter);
  }

  @Override
  protected BigInteger convertFrom(String s) {
    BigDecimal number = parseAsBigDecimal(s);
    if (number == null) {
      return null;
    }
    return number.toBigIntegerExact();
  }
}
