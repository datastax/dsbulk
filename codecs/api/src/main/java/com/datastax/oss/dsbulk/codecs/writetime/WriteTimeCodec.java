/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.codecs.writetime;

import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.util.CodecUtils;
import java.time.Instant;

/**
 * A special wrapper codec that maps write times to and from an external format.
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
