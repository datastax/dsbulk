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
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.concurrent.TimeUnit;

public class StringToInstantCodec extends StringToTemporalCodec<Instant> {

  private final ThreadLocal<DecimalFormat> numberFormat;
  private final TimeUnit timeUnit;
  private final ZonedDateTime epoch;

  public StringToInstantCodec(
      DateTimeFormatter temporalFormat,
      ThreadLocal<DecimalFormat> numberFormat,
      TimeUnit timeUnit,
      ZonedDateTime epoch) {
    super(InstantCodec.instance, temporalFormat);
    this.numberFormat = numberFormat;
    this.timeUnit = timeUnit;
    this.epoch = epoch;
  }

  @Override
  public Instant convertFrom(String s) {
    TemporalAccessor temporal = parseTemporalAccessor(s);
    if (temporal == null) {
      return null;
    }
    return CodecUtils.convertTemporal(
        temporal, Instant.class, temporalFormat.getZone(), epoch.toLocalDate());
  }

  @Override
  TemporalAccessor parseTemporalAccessor(String s) {
    // For timestamps, the conversion is more complex than for other temporals
    return CodecUtils.parseTemporal(
        s, temporalFormat, numberFormat.get(), timeUnit, epoch.toInstant());
  }
}
