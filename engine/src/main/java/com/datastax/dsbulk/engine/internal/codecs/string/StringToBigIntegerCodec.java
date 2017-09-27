/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.TypeCodec;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;

public class StringToBigIntegerCodec extends StringToNumberCodec<BigInteger> {

  public StringToBigIntegerCodec(ThreadLocal<DecimalFormat> formatter) {
    super(TypeCodec.varint(), formatter);
  }

  @Override
  public BigInteger convertFrom(String s) {
    BigDecimal number = parseAsBigDecimal(s);
    if (number == null) {
      return null;
    }
    return number.toBigIntegerExact();
  }
}
