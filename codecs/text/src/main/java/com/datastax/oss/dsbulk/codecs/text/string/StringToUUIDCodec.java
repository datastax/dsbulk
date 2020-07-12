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
package com.datastax.oss.dsbulk.codecs.text.string;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.api.util.CodecUtils;
import com.datastax.oss.dsbulk.codecs.api.util.TimeUUIDGenerator;
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
