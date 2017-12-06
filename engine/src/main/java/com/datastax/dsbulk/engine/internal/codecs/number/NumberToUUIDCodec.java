/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.number;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator;
import java.time.Instant;
import java.util.UUID;

public class NumberToUUIDCodec<FROM extends Number> extends ConvertingCodec<FROM, UUID> {

  private final NumberToInstantCodec<FROM> instantCodec;
  private final TimeUUIDGenerator generator;

  public NumberToUUIDCodec(
      TypeCodec<UUID> targetCodec,
      NumberToInstantCodec<FROM> instantCodec,
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
