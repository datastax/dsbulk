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

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public class JsonNodeToUnknownTypeCodec<T> extends JsonNodeConvertingCodec<T> {

  public JsonNodeToUnknownTypeCodec(TypeCodec<T> targetCodec, List<String> nullStrings) {
    super(targetCodec, nullStrings);
  }

  @Override
  public T externalToInternal(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    return getInternalCodec().parse(node.asText());
  }

  @Override
  public JsonNode internalToExternal(T o) {
    if (o == null) {
      return null;
    }
    String s = getInternalCodec().format(o);
    // most codecs usually format null/empty values using the CQL keyword "NULL",
    // but some others may choose to return a null string.
    if (s.equalsIgnoreCase("NULL")) {
      return null;
    }
    return JSON_NODE_FACTORY.textNode(s);
  }
}
