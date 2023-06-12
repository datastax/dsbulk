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
import com.datastax.oss.driver.api.core.type.CqlVectorType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.List;

public class JsonNodeToCqlVectorCodec<E> extends JsonNodeConvertingCodec<CqlVector<E>> {

  private final ConvertingCodec<JsonNode, E> eltCodec;
  private final ObjectMapper objectMapper;

  public JsonNodeToCqlVectorCodec(
      CqlVectorType cqlType,
      ConvertingCodec<JsonNode, E> eltCodec,
      TypeCodec<E> typeCodec,
      ObjectMapper objectMapper,
      List<String> nullStrings) {
    super(TypeCodecs.vectorOf(cqlType, typeCodec), nullStrings);
    this.eltCodec = eltCodec;
    this.objectMapper = objectMapper;
  }

  @Override
  public CqlVector<E> externalToInternal(JsonNode jsonNode) {
    if (jsonNode.isArray()) {
      CqlVector.Builder<E> builder = CqlVector.builder();
      jsonNode
          .elements()
          .forEachRemaining(
              item -> {
                E e = eltCodec.externalToInternal(item);
                builder.add(e);
              });
      return builder.build();
    } else {
      return null;
    }
  }

  @Override
  public JsonNode internalToExternal(CqlVector<E> value) {
    if (value == null) {
      return null;
    }
    ArrayNode root = objectMapper.createArrayNode();
    for (E element : value.getValues()) {
      root.add(eltCodec.internalToExternal(element));
    }
    return root;
  }
}
