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

import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.internal.core.type.codec.CqlVectorCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.Iterator;
import java.util.List;

public class JsonNodeToVectorCodec<SubtypeT> extends JsonNodeConvertingCodec<CqlVector<SubtypeT>> {

  private final ConvertingCodec<JsonNode, SubtypeT> subtypeCodec;
  private final ObjectMapper objectMapper;

  public JsonNodeToVectorCodec(
      CqlVectorCodec<SubtypeT> targetCodec,
      ConvertingCodec<JsonNode, SubtypeT> subtypeCodec,
      ObjectMapper objectMapper,
      List<String> nullStrings) {
    super(targetCodec, nullStrings);
    this.subtypeCodec = subtypeCodec;
    this.objectMapper = objectMapper;
  }

  @Override
  public CqlVector<SubtypeT> externalToInternal(JsonNode jsonNode) {
    if (jsonNode == null || !jsonNode.isArray()) return null;
    CqlVector.Builder<SubtypeT> builder = CqlVector.builder();
    for (Iterator<JsonNode> it = jsonNode.elements(); it.hasNext(); )
      builder.add(subtypeCodec.externalToInternal(it.next()));
    return builder.build();
  }

  @Override
  public JsonNode internalToExternal(CqlVector<SubtypeT> value) {
    if (value == null) return null;
    ArrayNode root = objectMapper.createArrayNode();
    for (SubtypeT element : value.getValues()) {
      root.add(subtypeCodec.internalToExternal(element));
    }
    return root;
  }
}
