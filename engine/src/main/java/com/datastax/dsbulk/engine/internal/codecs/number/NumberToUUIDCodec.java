/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
  public FROM internalToExternal(UUID value) {
    if (value == null) {
      return null;
    }
    Instant instant = TimeUUIDGenerator.fromUUIDTimestamp(value.timestamp());
    return instantCodec.internalToExternal(instant);
  }

  @Override
  public UUID externalToInternal(FROM value) {
    if (value == null) {
      return null;
    }
    Instant instant = instantCodec.externalToInternal(value);
    return generator.generate(instant);
  }
}
