/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs.text.string;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.dsbulk.codecs.util.CodecUtils;
import com.datastax.oss.dsbulk.codecs.util.OverflowStrategy;
import com.datastax.oss.dsbulk.codecs.util.TemporalFormat;
import io.netty.util.concurrent.FastThreadLocal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class StringToNumberCodec<N extends Number> extends StringConvertingCodec<N> {

  private final FastThreadLocal<NumberFormat> numberFormat;
  private final OverflowStrategy overflowStrategy;
  private final RoundingMode roundingMode;
  private final TemporalFormat temporalFormat;
  private final ZoneId timeZone;
  private final TimeUnit timeUnit;
  private final ZonedDateTime epoch;
  private final Map<String, Boolean> booleanStrings;
  private final List<N> booleanNumbers;

  StringToNumberCodec(
      TypeCodec<N> targetCodec,
      FastThreadLocal<NumberFormat> numberFormat,
      OverflowStrategy overflowStrategy,
      RoundingMode roundingMode,
      TemporalFormat temporalFormat,
      ZoneId timeZone,
      TimeUnit timeUnit,
      ZonedDateTime epoch,
      Map<String, Boolean> booleanStrings,
      List<N> booleanNumbers,
      List<String> nullStrings) {
    super(targetCodec, nullStrings);
    this.numberFormat = numberFormat;
    this.overflowStrategy = overflowStrategy;
    this.roundingMode = roundingMode;
    this.temporalFormat = temporalFormat;
    this.timeZone = timeZone;
    this.timeUnit = timeUnit;
    this.epoch = epoch;
    this.booleanStrings = booleanStrings;
    this.booleanNumbers = booleanNumbers;
  }

  @Override
  public String internalToExternal(N value) {
    if (value == null) {
      return nullString();
    }
    return CodecUtils.formatNumber(value, numberFormat.get());
  }

  Number parseNumber(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    return CodecUtils.parseNumber(
        s,
        numberFormat.get(),
        temporalFormat,
        timeZone,
        timeUnit,
        epoch,
        booleanStrings,
        booleanNumbers);
  }

  N narrowNumber(Number number, Class<? extends N> targetClass) {
    return CodecUtils.narrowNumber(number, targetClass, overflowStrategy, roundingMode);
  }
}
