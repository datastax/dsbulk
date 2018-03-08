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
import java.util.List;
import java.util.UUID;

public class StringToUUIDCodec extends ConvertingCodec<String, UUID> {

  private final ConvertingCodec<String, Instant> instantCodec;
  private final TimeUUIDGenerator generator;
  private final List<String> nullWords;

  public StringToUUIDCodec(
      TypeCodec<UUID> targetCodec,
      ConvertingCodec<String, Instant> instantCodec,
      TimeUUIDGenerator generator,
      List<String> nullWords) {
    super(targetCodec, String.class);
    this.instantCodec = instantCodec;
    this.generator = generator;
    this.nullWords = nullWords;
  }

  @Override
  public UUID externalToInternal(String s) {
    if (s == null || s.isEmpty() || nullWords.contains(s)) {
      return null;
    }
    return CodecUtils.parseUUID(s, instantCodec, generator);
  }

  @Override
  public String internalToExternal(UUID value) {
    if (value == null) {
      return nullWords.isEmpty() ? null : nullWords.get(0);
    }
    return value.toString();
  }
}
