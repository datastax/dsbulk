/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.writetime;

import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import java.time.Instant;

public class WriteTimeCodec<T> extends ConvertingCodec<T, Long> {

  private final ConvertingCodec<T, Instant> innerCodec;

  public WriteTimeCodec(ConvertingCodec<T, Instant> innerCodec) {
    super(bigint(), innerCodec.getJavaType());
    this.innerCodec = innerCodec;
  }

  @Override
  public Long convertFrom(T value) {
    if (value == null) {
      return null;
    }
    Instant i = innerCodec.convertFrom(value);
    return CodecUtils.instantToNumber(i, MICROSECONDS, EPOCH);
  }

  @Override
  public T convertTo(Long value) {
    throw new UnsupportedOperationException("This codec cannot be used when deserializing");
  }
}
