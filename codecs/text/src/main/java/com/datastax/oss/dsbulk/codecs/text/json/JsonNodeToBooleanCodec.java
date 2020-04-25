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
package com.datastax.oss.dsbulk.codecs.text.json;

import static com.datastax.oss.dsbulk.codecs.text.json.JsonCodecUtils.JSON_NODE_FACTORY;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;

public class JsonNodeToBooleanCodec extends JsonNodeConvertingCodec<Boolean> {

  private final Map<String, Boolean> inputs;

  public JsonNodeToBooleanCodec(Map<String, Boolean> inputs, List<String> nullStrings) {
    super(TypeCodecs.BOOLEAN, nullStrings);
    this.inputs = inputs;
  }

  @Override
  public Boolean externalToInternal(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    if (node.isBoolean()) {
      return node.asBoolean();
    }
    String s = node.asText();
    Boolean b = inputs.get(s.toLowerCase());
    if (b == null) {
      throw new IllegalArgumentException("Invalid boolean value: " + s);
    }
    return b;
  }

  @Override
  public JsonNode internalToExternal(Boolean value) {
    return value == null ? null : JSON_NODE_FACTORY.booleanNode(value);
  }
}
