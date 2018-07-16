/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
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
