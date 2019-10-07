/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.writetime;

import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.dsbulk.commons.codecs.util.CodecUtils;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import java.time.Instant;

/**
 * A special codec that maps write times to and from an external format.
 *
 * <p>Write times are inserted using an {@code USING TIMESTAMP} clause and can be retrieved using
 * the {@code WRITETIME} function, as in {@code writetime(mycol)}. Write times are always expressed
 * internally as bigints representing microseconds since the Unix Epoch.
 *
 * @param <T> The external type.
 */
public class WriteTimeCodec<T> extends ConvertingCodec<T, Long> {

  private final ConvertingCodec<T, Instant> innerCodec;

  public WriteTimeCodec(ConvertingCodec<T, Instant> innerCodec) {
    super(TypeCodecs.BIGINT, innerCodec.getJavaType());
    this.innerCodec = innerCodec;
  }

  @Override
  public Long externalToInternal(T value) {
    if (value == null) {
      return null;
    }
    Instant i = innerCodec.externalToInternal(value);
    if (i == null) {
      return null;
    }
    return CodecUtils.instantToNumber(i, MICROSECONDS, EPOCH);
  }

  @Override
  public T internalToExternal(Long value) {
    if (value == null) {
      return null;
    }
    Instant i = CodecUtils.numberToInstant(value, MICROSECONDS, EPOCH);
    return innerCodec.internalToExternal(i);
  }
}
