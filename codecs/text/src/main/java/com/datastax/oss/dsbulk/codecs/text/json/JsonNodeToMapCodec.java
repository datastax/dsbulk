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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JsonNodeToMapCodec<K, V> extends JsonNodeConvertingCodec<Map<K, V>> {

  private final ConvertingCodec<String, K> keyCodec;
  private final ConvertingCodec<JsonNode, V> valueCodec;
  private final ObjectMapper objectMapper;
  private final Map<K, V> emptyMap;

  public JsonNodeToMapCodec(
      TypeCodec<Map<K, V>> collectionCodec,
      ConvertingCodec<String, K> keyCodec,
      ConvertingCodec<JsonNode, V> valueCodec,
      ObjectMapper objectMapper,
      List<String> nullStrings) {
    super(collectionCodec, nullStrings);
    this.keyCodec = keyCodec;
    this.valueCodec = valueCodec;
    this.objectMapper = objectMapper;
    emptyMap = ImmutableMap.of();
  }

  @Override
  public Map<K, V> externalToInternal(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    if (!node.isObject()) {
      throw new IllegalArgumentException("Expecting OBJECT node, got " + node.getNodeType());
    }
    if (node.size() == 0) {
      return emptyMap;
    }
    Map<K, V> map = new LinkedHashMap<>(node.size());
    Iterator<Map.Entry<String, JsonNode>> it = node.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> entry = it.next();
      map.put(
          keyCodec.externalToInternal(entry.getKey()),
          valueCodec.externalToInternal(entry.getValue()));
    }
    return map;
  }

  @Override
  public JsonNode internalToExternal(Map<K, V> map) {
    if (map == null) {
      return null;
    }
    ObjectNode root = objectMapper.createObjectNode();
    for (Map.Entry<K, V> entry : map.entrySet()) {
      String key = keyCodec.internalToExternal(entry.getKey());
      JsonNode value = valueCodec.internalToExternal(entry.getValue());
      root.set(key, value);
    }
    return root;
  }
}
