/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class StringToBigIntegerCodec extends StringToNumberCodec<BigInteger> {

  public StringToBigIntegerCodec(
      ThreadLocal<DecimalFormat> formatter,
      DateTimeFormatter temporalParser,
      TimeUnit numericTimestampUnit,
      Instant numericTimestampEpoch) {
    super(
        TypeCodec.varint(), formatter, temporalParser, numericTimestampUnit, numericTimestampEpoch);
  }

  @Override
  public BigInteger convertFrom(String s) {
    Number number =
        CodecUtils.parseNumber(
            s, getNumberFormat(), temporalParser, numericTimestampUnit, numericTimestampEpoch);
    if (number == null) {
      return null;
    }
    return CodecUtils.toBigIntegerExact(number);
  }
}
