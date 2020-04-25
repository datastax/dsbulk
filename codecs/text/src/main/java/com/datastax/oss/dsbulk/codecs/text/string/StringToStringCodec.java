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
import java.util.List;

public class StringToStringCodec extends StringConvertingCodec<String> {

  public StringToStringCodec(TypeCodec<String> innerCodec, List<String> nullStrings) {
    super(innerCodec, nullStrings);
  }

  @Override
  public String externalToInternal(String s) {
    // DAT-297: do not convert empty strings to null so do not use isNullOrEmpty() here
    if (isNull(s)) {
      return null;
    }
    return s;
  }

  @Override
  public String internalToExternal(String value) {
    if (value == null) {
      return nullString();
    }
    return value;
  }
}
