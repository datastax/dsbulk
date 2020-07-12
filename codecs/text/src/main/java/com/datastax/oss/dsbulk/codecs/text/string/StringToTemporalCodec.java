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
import com.datastax.oss.dsbulk.codecs.api.util.TemporalFormat;
import java.time.temporal.TemporalAccessor;
import java.util.List;

public abstract class StringToTemporalCodec<T extends TemporalAccessor>
    extends StringConvertingCodec<T> {

  final TemporalFormat temporalFormat;

  StringToTemporalCodec(
      TypeCodec<T> targetCodec, TemporalFormat temporalFormat, List<String> nullStrings) {
    super(targetCodec, nullStrings);
    this.temporalFormat = temporalFormat;
  }

  @Override
  public String internalToExternal(T value) {
    if (value == null) {
      return nullString();
    }
    return temporalFormat.format(value);
  }

  TemporalAccessor parseTemporalAccessor(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    return temporalFormat.parse(s);
  }
}
