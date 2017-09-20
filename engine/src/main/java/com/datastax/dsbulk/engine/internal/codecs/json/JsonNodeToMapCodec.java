/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class JsonNodeToMapCodec<K, V> extends ConvertingCodec<JsonNode, Map<K, V>> {

  private final ConvertingCodec<String, K> keyCodec;
  private final ConvertingCodec<JsonNode, V> valueCodec;
  private final ObjectMapper objectMapper;

  public JsonNodeToMapCodec(
      TypeCodec<Map<K, V>> collectionCodec,
      ConvertingCodec<String, K> keyCodec,
      ConvertingCodec<JsonNode, V> valueCodec,
      ObjectMapper objectMapper) {
    super(collectionCodec, JsonNode.class);
    this.keyCodec = keyCodec;
    this.valueCodec = valueCodec;
    this.objectMapper = objectMapper;
  }

  @Override
  public Map<K, V> convertFrom(JsonNode node) {
    if (node == null || node.isNull() || node.size() == 0) {
      return null;
    }
    Map<K, V> map = new LinkedHashMap<>(node.size());
    Iterator<Map.Entry<String, JsonNode>> it = node.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> entry = it.next();
      map.put(keyCodec.convertFrom(entry.getKey()), valueCodec.convertFrom(entry.getValue()));
    }
    return map;
  }

  @Override
  public JsonNode convertTo(Map<K, V> map) {
    if (map == null) {
      return null;
    }
    ObjectNode root = objectMapper.createObjectNode();
    for (Map.Entry<K, V> entry : map.entrySet()) {
      String key = keyCodec.convertTo(entry.getKey());
      JsonNode value = valueCodec.convertTo(entry.getValue());
      root.set(key, value);
    }
    return root;
  }
}
