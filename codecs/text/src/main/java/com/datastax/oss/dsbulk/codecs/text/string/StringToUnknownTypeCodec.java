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

public class StringToUnknownTypeCodec<T> extends StringConvertingCodec<T> {

  public StringToUnknownTypeCodec(TypeCodec<T> targetCodec, List<String> nullStrings) {
    super(targetCodec, nullStrings);
  }

  @Override
  public T externalToInternal(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    return getInternalCodec().parse(s);
  }

  @Override
  public String internalToExternal(T o) {
    if (o == null) {
      return nullString();
    }
    String s = getInternalCodec().format(o);
    // most codecs usually format null/empty values using the CQL keyword "NULL"
    if (s.equalsIgnoreCase("NULL")) {
      return nullString();
    }
    return s;
  }
}
