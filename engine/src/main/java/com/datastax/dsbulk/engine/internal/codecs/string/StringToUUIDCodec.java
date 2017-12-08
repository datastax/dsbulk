/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator;
import java.time.Instant;
import java.util.UUID;

public class StringToUUIDCodec extends ConvertingCodec<String, UUID> {

  private final ConvertingCodec<String, Instant> instantCodec;
  private final TimeUUIDGenerator generator;

  public StringToUUIDCodec(
      TypeCodec<UUID> targetCodec,
      ConvertingCodec<String, Instant> instantCodec,
      TimeUUIDGenerator generator) {
    super(targetCodec, String.class);
    this.instantCodec = instantCodec;
    this.generator = generator;
  }

  @Override
  public UUID convertFrom(String s) {
    return CodecUtils.parseUUID(s, instantCodec, generator);
  }

  @Override
  public String convertTo(UUID value) {
    if (value == null) {
      return null;
    }
    return value.toString();
  }
}
