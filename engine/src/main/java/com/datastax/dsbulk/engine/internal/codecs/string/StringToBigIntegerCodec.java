/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static java.util.stream.Collectors.toList;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StringToBigIntegerCodec extends StringToNumberCodec<BigInteger> {

  public StringToBigIntegerCodec(
      ThreadLocal<DecimalFormat> formatter,
      DateTimeFormatter temporalParser,
      TimeUnit numericTimestampUnit,
      Instant numericTimestampEpoch,
      Map<String, Boolean> booleanWords,
      List<BigDecimal> booleanNumbers) {
    super(
        TypeCodec.varint(),
        formatter,
        temporalParser,
        numericTimestampUnit,
        numericTimestampEpoch,
        booleanWords,
        booleanNumbers.stream().map(BigDecimal::toBigInteger).collect(toList()));
  }

  @Override
  public BigInteger convertFrom(String s) {
    Number number = parseNumber(s);
    if (number == null) {
      return null;
    }
    return CodecUtils.toBigIntegerExact(number);
  }
}
