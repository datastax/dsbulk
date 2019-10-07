/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.string;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.dsbulk.commons.codecs.util.CodecUtils;
import com.datastax.dsbulk.commons.codecs.util.TimeUUIDGenerator;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class StringToUUIDCodec extends StringConvertingCodec<UUID> {

  private final ConvertingCodec<String, Instant> instantCodec;
  private final TimeUUIDGenerator generator;

  public StringToUUIDCodec(
      TypeCodec<UUID> targetCodec,
      ConvertingCodec<String, Instant> instantCodec,
      TimeUUIDGenerator generator,
      List<String> nullStrings) {
    super(targetCodec, nullStrings);
    this.instantCodec = instantCodec;
    this.generator = generator;
  }

  @Override
  public UUID externalToInternal(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    return CodecUtils.parseUUID(s, instantCodec, generator);
  }

  @Override
  public String internalToExternal(UUID value) {
    if (value == null) {
      return nullString();
    }
    return value.toString();
  }
}
