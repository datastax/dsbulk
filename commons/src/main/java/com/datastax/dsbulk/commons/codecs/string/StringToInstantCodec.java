/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.string;

import com.datastax.dsbulk.commons.codecs.util.CodecUtils;
import com.datastax.dsbulk.commons.codecs.util.TemporalFormat;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
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
    super(TypeCodecs.TIMESTAMP, temporalFormat, nullStrings);
    this.timeZone = timeZone;
    this.epoch = epoch;
  }

  @Override
  public Instant externalToInternal(String s) {
    TemporalAccessor temporal = parseTemporalAccessor(s);
    if (temporal == null) {
      return null;
    }
    return CodecUtils.toInstant(temporal, timeZone, epoch.toLocalDate());
  }
}
