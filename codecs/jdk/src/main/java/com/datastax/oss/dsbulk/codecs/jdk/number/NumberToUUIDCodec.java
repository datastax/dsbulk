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

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.api.util.TimeUUIDGenerator;
import java.time.Instant;
import java.util.UUID;

public class NumberToUUIDCodec<EXTERNAL extends Number> extends ConvertingCodec<EXTERNAL, UUID> {

  private final NumberToInstantCodec<EXTERNAL> instantCodec;
  private final TimeUUIDGenerator generator;

  @SuppressWarnings("unchecked")
  public NumberToUUIDCodec(
      TypeCodec<UUID> targetCodec,
      NumberToInstantCodec<EXTERNAL> instantCodec,
      TimeUUIDGenerator generator) {
    super(targetCodec, (Class<EXTERNAL>) instantCodec.getJavaType().getRawType());
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
