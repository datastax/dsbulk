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

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;

public class StringToUDTCodec extends StringConvertingCodec<UdtValue> {

  private final ConvertingCodec<JsonNode, UdtValue> jsonCodec;
  private final ObjectMapper objectMapper;

  public StringToUDTCodec(
      ConvertingCodec<JsonNode, UdtValue> jsonCodec,
      ObjectMapper objectMapper,
      List<String> nullStrings) {
    super(jsonCodec.getInternalCodec(), nullStrings);
    this.jsonCodec = jsonCodec;
    this.objectMapper = objectMapper;
  }

  @Override
  public UdtValue externalToInternal(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    try {
      JsonNode node = objectMapper.readTree(s);
      return jsonCodec.externalToInternal(node);
    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("Could not parse '%s' as Json", s), e);
    }
  }

  @Override
  public String internalToExternal(UdtValue udt) {
    if (udt == null) {
      return nullString();
    }
    try {
      JsonNode node = jsonCodec.internalToExternal(udt);
      return objectMapper.writeValueAsString(node);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(String.format("Could not format '%s' to Json", udt), e);
    }
  }
}
