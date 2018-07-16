/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.temporal;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.dsbulk.commons.codecs.util.TimeUUIDGenerator;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;

public class DateToUUIDCodec<EXTERNAL extends Date> extends ConvertingCodec<EXTERNAL, UUID> {

  private final DateToTemporalCodec<EXTERNAL, Instant> instantCodec;
  private final TimeUUIDGenerator generator;

  public DateToUUIDCodec(
      TypeCodec<UUID> targetCodec,
      DateToTemporalCodec<EXTERNAL, Instant> instantCodec,
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
