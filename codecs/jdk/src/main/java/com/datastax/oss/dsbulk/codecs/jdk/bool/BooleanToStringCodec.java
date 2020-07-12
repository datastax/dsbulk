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
package com.datastax.oss.dsbulk.codecs.jdk.bool;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;

public class BooleanToStringCodec extends ConvertingCodec<Boolean, String> {

  public BooleanToStringCodec() {
    super(TypeCodecs.TEXT, Boolean.class);
  }

  @Override
  public Boolean internalToExternal(String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }
    throw new UnsupportedOperationException(
        "This codec does not support converting from string to boolean");
  }

  @Override
  public String externalToInternal(Boolean value) {
    if (value == null) {
      return null;
    }
    return value.toString();
  }
}
