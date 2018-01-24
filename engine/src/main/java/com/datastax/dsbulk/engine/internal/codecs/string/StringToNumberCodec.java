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
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class StringToNumberCodec<N extends Number> extends ConvertingCodec<String, N> {

  private final ThreadLocal<DecimalFormat> formatter;
  private final DateTimeFormatter temporalParser;
  private final TimeUnit numericTimestampUnit;
  private final Instant numericTimestampEpoch;
  private final Map<String, Boolean> booleanWords;
  private final List<N> booleanNumbers;

  StringToNumberCodec(
      TypeCodec<N> targetCodec,
      ThreadLocal<DecimalFormat> formatter,
      DateTimeFormatter temporalParser,
      TimeUnit numericTimestampUnit,
      Instant numericTimestampEpoch,
      Map<String, Boolean> booleanWords,
      List<N> booleanNumbers) {
    super(targetCodec, String.class);
    this.formatter = formatter;
    this.temporalParser = temporalParser;
    this.numericTimestampUnit = numericTimestampUnit;
    this.numericTimestampEpoch = numericTimestampEpoch;
    this.booleanWords = booleanWords;
    this.booleanNumbers = booleanNumbers;
  }

  @Override
  public String convertTo(N value) {
    if (value == null) {
      return null;
    }
    return getNumberFormat().format(value);
  }

  Number parseNumber(String s) {
    return CodecUtils.parseNumber(
        s,
        getNumberFormat(),
        temporalParser,
        numericTimestampUnit,
        numericTimestampEpoch,
        booleanWords,
        booleanNumbers);
  }

  private DecimalFormat getNumberFormat() {
    DecimalFormat format = formatter.get();
    format.setParseBigDecimal(true);
    return format;
  }
}
