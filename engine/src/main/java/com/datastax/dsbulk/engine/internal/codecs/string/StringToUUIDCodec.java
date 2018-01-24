/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
