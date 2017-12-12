/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.writetime;

import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.instantToTimestampSinceEpoch;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import java.time.Instant;

/** */
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
    return instantToTimestampSinceEpoch(i, MICROSECONDS, EPOCH);
  }

  @Override
  public T convertTo(Long value) {
    throw new UnsupportedOperationException("This codec cannot be used when deserializing");
  }
}
