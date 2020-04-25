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

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import java.util.List;
import java.util.Map;

public class StringToBooleanCodec extends StringConvertingCodec<Boolean> {

  private final Map<String, Boolean> inputs;
  private final Map<Boolean, String> outputs;

  public StringToBooleanCodec(
      Map<String, Boolean> inputs, Map<Boolean, String> outputs, List<String> nullStrings) {
    super(TypeCodecs.BOOLEAN, nullStrings);
    this.inputs = inputs;
    this.outputs = outputs;
  }

  @Override
  public Boolean externalToInternal(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    Boolean b = inputs.get(s.toLowerCase());
    if (b == null) {
      throw new IllegalArgumentException("Invalid boolean value: " + s);
    }
    return b;
  }

  @Override
  public String internalToExternal(Boolean value) {
    if (value == null) {
      return nullString();
    }
    String s = outputs.get(value);
    if (s == null) {
      return Boolean.toString(value);
    }
    return s;
  }
}
