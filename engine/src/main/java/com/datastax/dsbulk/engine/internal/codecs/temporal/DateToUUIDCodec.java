/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.temporal;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;

public class DateToUUIDCodec<FROM extends Date> extends ConvertingCodec<FROM, UUID> {

  private final DateToTemporalCodec<FROM, Instant> instantCodec;
  private final TimeUUIDGenerator generator;

  public DateToUUIDCodec(
      TypeCodec<UUID> targetCodec,
      DateToTemporalCodec<FROM, Instant> instantCodec,
      TimeUUIDGenerator generator) {
    super(targetCodec, instantCodec.getJavaType());
    this.instantCodec = instantCodec;
    this.generator = generator;
  }

  @Override
  public FROM convertTo(UUID value) {
    if (value == null) {
      return null;
    }
    Instant instant = TimeUUIDGenerator.fromUUIDTimestamp(value.timestamp());
    return instantCodec.convertTo(instant);
  }

  @Override
  public UUID convertFrom(FROM value) {
    if (value == null) {
      return null;
    }
    Instant instant = instantCodec.convertFrom(value);
    return generator.generate(instant);
  }
}
