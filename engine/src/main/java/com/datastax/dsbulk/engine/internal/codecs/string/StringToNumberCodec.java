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
import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class StringToNumberCodec<N extends Number> extends ConvertingCodec<String, N> {

  private final ThreadLocal<DecimalFormat> numberFormat;
  private final OverflowStrategy overflowStrategy;
  private final RoundingMode roundingMode;
  private final DateTimeFormatter temporalFormat;
  private final TimeUnit timeUnit;
  private final ZonedDateTime epoch;
  private final Map<String, Boolean> booleanWords;
  private final List<N> booleanNumbers;

  StringToNumberCodec(
      TypeCodec<N> targetCodec,
      ThreadLocal<DecimalFormat> numberFormat,
      OverflowStrategy overflowStrategy,
      RoundingMode roundingMode,
      DateTimeFormatter temporalFormat,
      TimeUnit timeUnit,
      ZonedDateTime epoch,
      Map<String, Boolean> booleanWords,
      List<N> booleanNumbers) {
    super(targetCodec, String.class);
    this.numberFormat = numberFormat;
    this.overflowStrategy = overflowStrategy;
    this.roundingMode = roundingMode;
    this.temporalFormat = temporalFormat;
    this.timeUnit = timeUnit;
    this.epoch = epoch;
    this.booleanWords = booleanWords;
    this.booleanNumbers = booleanNumbers;
  }

  @Override
  public String convertTo(N value) {
    if (value == null) {
      return null;
    }
    return CodecUtils.formatNumber(value, numberFormat.get());
  }

  Number parseNumber(String s) {
    return CodecUtils.parseNumber(
        s, numberFormat.get(), temporalFormat, timeUnit, epoch, booleanWords, booleanNumbers);
  }

  N narrowNumber(Number number, Class<? extends N> targetClass) {
    return CodecUtils.narrowNumber(number, targetClass, overflowStrategy, roundingMode);
  }
}
