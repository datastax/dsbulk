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
import com.datastax.dsbulk.engine.internal.codecs.util.TemporalFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.List;

public class StringToInstantCodec extends StringToTemporalCodec<Instant> {

  private final ZoneId timeZone;
  private final ZonedDateTime epoch;

  public StringToInstantCodec(
      TemporalFormat temporalFormat,
      ZoneId timeZone,
      ZonedDateTime epoch,
      List<String> nullStrings) {
    super(InstantCodec.instance, temporalFormat, nullStrings);
    this.timeZone = timeZone;
    this.epoch = epoch;
  }

  @Override
  public Instant externalToInternal(String s) {
    TemporalAccessor temporal = parseTemporalAccessor(s);
    if (temporal == null) {
      return null;
    }
    return CodecUtils.convertTemporal(temporal, Instant.class, timeZone, epoch.toLocalDate());
  }

  @Override
  TemporalAccessor parseTemporalAccessor(String s) {
    if (isNull(s)) {
      return null;
    }
    // For timestamps, the conversion is more complex than for other temporals
    return temporalFormat.parse(s);
  }
}
