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

import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.commons.StringUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class StringToMapCodec<K, V> extends StringConvertingCodec<Map<K, V>> {

  private final ConvertingCodec<JsonNode, Map<K, V>> jsonCodec;
  private final ObjectMapper objectMapper;

  public StringToMapCodec(
      ConvertingCodec<JsonNode, Map<K, V>> jsonCodec,
      ObjectMapper objectMapper,
      List<String> nullStrings) {
    super(jsonCodec.getInternalCodec(), nullStrings);
    this.jsonCodec = jsonCodec;
    this.objectMapper = objectMapper;
  }

  @Override
  public Map<K, V> externalToInternal(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    try {
      JsonNode node = objectMapper.readTree(StringUtils.ensureBraces(s));
      return jsonCodec.externalToInternal(node);
    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("Could not parse '%s' as Json", s), e);
    }
  }

  @Override
  public String internalToExternal(Map<K, V> map) {
    if (map == null) {
      return nullString();
    }
    try {
      JsonNode node = jsonCodec.internalToExternal(map);
      return objectMapper.writeValueAsString(node);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(String.format("Could not format '%s' to Json", map), e);
    }
  }
}
