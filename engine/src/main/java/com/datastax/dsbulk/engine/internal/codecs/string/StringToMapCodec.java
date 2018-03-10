/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToMapCodec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class StringToMapCodec<K, V> extends ConvertingCodec<String, Map<K, V>> {

  private final JsonNodeToMapCodec<K, V> jsonCodec;
  private final ObjectMapper objectMapper;
  private final List<String> nullWords;

  public StringToMapCodec(
      JsonNodeToMapCodec<K, V> jsonCodec, ObjectMapper objectMapper, List<String> nullWords) {
    super(jsonCodec.getTargetCodec(), String.class);
    this.jsonCodec = jsonCodec;
    this.objectMapper = objectMapper;
    this.nullWords = nullWords;
  }

  @Override
  public Map<K, V> convertFrom(String s) {
    if (s == null || s.isEmpty() || nullWords.contains(s)) {
      return null;
    }
    try {
      JsonNode node = objectMapper.readTree(s);
      return jsonCodec.convertFrom(node);
    } catch (IOException e) {
      throw new InvalidTypeException(String.format("Could not parse '%s' as Json", s), e);
    }
  }

  @Override
  public String convertTo(Map<K, V> map) {
    if (map == null) {
      return nullWords.isEmpty() ? null : nullWords.get(0);
    }
    try {
      JsonNode node = jsonCodec.convertTo(map);
      return objectMapper.writeValueAsString(node);
    } catch (JsonProcessingException e) {
      throw new InvalidTypeException(String.format("Could not format '%s' to Json", map), e);
    }
  }
}
