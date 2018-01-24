/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StringToBigDecimalCodec extends StringToNumberCodec<BigDecimal> {

  public StringToBigDecimalCodec(
      ThreadLocal<DecimalFormat> formatter,
      DateTimeFormatter temporalParser,
      TimeUnit numericTimestampUnit,
      Instant numericTimestampEpoch,
      Map<String, Boolean> booleanWords,
      List<BigDecimal> booleanNumbers) {
    super(
        TypeCodec.decimal(),
        formatter,
        temporalParser,
        numericTimestampUnit,
        numericTimestampEpoch,
        booleanWords,
        booleanNumbers);
  }

  @Override
  public BigDecimal convertFrom(String s) {
    Number number = parseNumber(s);
    if (number == null) {
      return null;
    }
    return CodecUtils.toBigDecimal(number);
  }
}
