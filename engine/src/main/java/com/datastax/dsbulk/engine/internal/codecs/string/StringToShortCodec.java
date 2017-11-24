/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import java.math.BigDecimal;
import java.text.DecimalFormat;

public class StringToShortCodec extends StringToNumberCodec<Short> {

  public StringToShortCodec(ThreadLocal<DecimalFormat> formatter) {
    super(smallInt(), formatter);
  }

  @Override
  public Short convertFrom(String s) {
    BigDecimal number = parseAsBigDecimal(s);
    if (number == null) {
      return null;
    }
    return number.shortValueExact();
  }
}
