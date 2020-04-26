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
package com.datastax.oss.dsbulk.codecs.jdk.number;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.util.CodecUtils;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;

public class NumberToInstantCodec<EXTERNAL extends Number>
    extends ConvertingCodec<EXTERNAL, Instant> {

  private final TimeUnit timeUnit;
  private final ZonedDateTime epoch;

  public NumberToInstantCodec(Class<EXTERNAL> javaType, TimeUnit timeUnit, ZonedDateTime epoch) {
    super(TypeCodecs.TIMESTAMP, javaType);
    this.timeUnit = timeUnit;
    this.epoch = epoch;
  }

  @Override
  public EXTERNAL internalToExternal(Instant value) {
    if (value == null) {
      return null;
    }
    long timestamp = CodecUtils.instantToNumber(value, timeUnit, epoch.toInstant());
    @SuppressWarnings("unchecked")
    EXTERNAL n = CodecUtils.convertNumber(timestamp, (Class<EXTERNAL>) getJavaType().getRawType());
    return n;
  }

  @Override
  public Instant externalToInternal(EXTERNAL value) {
    if (value == null) {
      return null;
    }
    return CodecUtils.numberToInstant(value, timeUnit, epoch.toInstant());
  }
}
