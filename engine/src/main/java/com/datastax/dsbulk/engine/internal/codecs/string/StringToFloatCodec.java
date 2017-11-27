/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class StringToFloatCodec extends StringToNumberCodec<Float> {

  public StringToFloatCodec(
      ThreadLocal<DecimalFormat> formatter,
      DateTimeFormatter temporalParser,
      TimeUnit numericTimestampUnit,
      Instant numericTimestampEpoch) {
    super(
        TypeCodec.cfloat(), formatter, temporalParser, numericTimestampUnit, numericTimestampEpoch);
  }

  @Override
  public Float convertFrom(String s) {
    Number number =
        CodecUtils.parseNumber(
            s, getNumberFormat(), temporalParser, numericTimestampUnit, numericTimestampEpoch);
    if (number == null) {
      return null;
    }
    return CodecUtils.toFloatValue(number);
  }
}
