/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import io.netty.util.concurrent.FastThreadLocal;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class StringToInstantCodec extends StringToTemporalCodec<Instant> {

  private final FastThreadLocal<NumberFormat> numberFormat;
  private final TimeUnit timeUnit;
  private final ZonedDateTime epoch;

  public StringToInstantCodec(
      DateTimeFormatter temporalFormat,
      FastThreadLocal<NumberFormat> numberFormat,
      TimeUnit timeUnit,
      ZonedDateTime epoch,
      List<String> nullStrings) {
    super(InstantCodec.instance, temporalFormat, nullStrings);
    this.numberFormat = numberFormat;
    this.timeUnit = timeUnit;
    this.epoch = epoch;
  }

  @Override
  public Instant externalToInternal(String s) {
    TemporalAccessor temporal = parseTemporalAccessor(s);
    if (temporal == null) {
      return null;
    }
    return CodecUtils.convertTemporal(
        temporal, Instant.class, temporalFormat.getZone(), epoch.toLocalDate());
  }

  @Override
  TemporalAccessor parseTemporalAccessor(String s) {
    if (s == null || s.isEmpty() || nullStrings.contains(s)) {
      return null;
    }
    // For timestamps, the conversion is more complex than for other temporals
    return CodecUtils.parseTemporal(
        s, temporalFormat, numberFormat.get(), timeUnit, epoch.toInstant());
  }
}
