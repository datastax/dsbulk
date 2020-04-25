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

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import java.util.List;

public class StringToDurationCodec extends StringConvertingCodec<CqlDuration> {

  public StringToDurationCodec(List<String> nullStrings) {
    super(TypeCodecs.DURATION, nullStrings);
  }

  @Override
  public CqlDuration externalToInternal(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    return CqlDuration.from(s);
  }

  @Override
  public String internalToExternal(CqlDuration value) {
    if (value == null) {
      return nullString();
    }
    return value.toString();
  }
}
