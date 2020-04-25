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

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;

public class JsonNodeToStringCodec extends JsonNodeConvertingCodec<String> {

  private final ObjectMapper objectMapper;

  public JsonNodeToStringCodec(
      TypeCodec<String> innerCodec, ObjectMapper objectMapper, List<String> nullStrings) {
    super(innerCodec, nullStrings);
    this.objectMapper = objectMapper;
  }

  @Override
  public String externalToInternal(JsonNode node) {
    // DAT-297: do not convert empty strings to null so do not use isNullOrEmpty() here
    if (isNull(node)) {
      return null;
    }
    if (node.isContainerNode()) {
      try {
        return objectMapper.writeValueAsString(node);
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Cannot deserialize node " + node, e);
      }
    } else {
      return node.asText();
    }
  }

  @Override
  public JsonNode internalToExternal(String value) {
    if (value == null) {
      return null;
    }
    return objectMapper.getNodeFactory().textNode(value);
  }
}
