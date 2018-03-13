/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.temporal;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator;
import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.UUID;

public class TemporalToUUIDCodec<EXTERNAL extends TemporalAccessor>
    extends ConvertingCodec<EXTERNAL, UUID> {

  private final TemporalToTemporalCodec<EXTERNAL, Instant> instantCodec;
  private final TimeUUIDGenerator generator;

  public TemporalToUUIDCodec(
      TypeCodec<UUID> targetCodec,
      TemporalToTemporalCodec<EXTERNAL, Instant> instantCodec,
      TimeUUIDGenerator generator) {
    super(targetCodec, instantCodec.getJavaType());
    this.instantCodec = instantCodec;
    this.generator = generator;
  }

  @Override
  public EXTERNAL internalToExternal(UUID value) {
    if (value == null) {
      return null;
    }
    Instant instant = TimeUUIDGenerator.fromUUIDTimestamp(value.timestamp());
    return instantCodec.internalToExternal(instant);
  }

  @Override
  public UUID externalToInternal(EXTERNAL value) {
    if (value == null) {
      return null;
    }
    Instant instant = instantCodec.externalToInternal(value);
    return generator.generate(instant);
  }
}
