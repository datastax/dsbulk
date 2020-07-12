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

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.List;

public class JsonNodeToTupleCodec extends JsonNodeConvertingCodec<TupleValue> {

  private final TupleType definition;
  private final List<ConvertingCodec<JsonNode, Object>> eltCodecs;
  private final ObjectMapper objectMapper;
  private final boolean allowExtraFields;
  private final boolean allowMissingFields;

  public JsonNodeToTupleCodec(
      TypeCodec<TupleValue> tupleCodec,
      List<ConvertingCodec<JsonNode, Object>> eltCodecs,
      ObjectMapper objectMapper,
      List<String> nullStrings,
      boolean allowExtraFields,
      boolean allowMissingFields) {
    super(tupleCodec, nullStrings);
    this.eltCodecs = eltCodecs;
    definition = (TupleType) tupleCodec.getCqlType();
    this.objectMapper = objectMapper;
    this.allowExtraFields = allowExtraFields;
    this.allowMissingFields = allowMissingFields;
  }

  @Override
  public TupleValue externalToInternal(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    if (!node.isArray()) {
      throw new IllegalArgumentException("Expecting ARRAY node, got " + node.getNodeType());
    }
    int tupleSize = definition.getComponentTypes().size();
    int nodeSize = node.size();
    if (nodeSize > tupleSize && !allowExtraFields) {
      throw JsonSchemaMismatchException.arraySizeGreaterThanTupleSize(tupleSize, nodeSize);
    }
    if (nodeSize < tupleSize && !allowMissingFields) {
      throw JsonSchemaMismatchException.arraySizeLesserThanTupleSize(tupleSize, nodeSize);
    }
    TupleValue tuple = definition.newValue();
    for (int i = 0; i < tupleSize && i < nodeSize; i++) {
      ConvertingCodec<JsonNode, Object> eltCodec = eltCodecs.get(i);
      Object o = eltCodec.externalToInternal(node.get(i));
      tuple = tuple.set(i, o, eltCodec.getInternalJavaType());
    }
    return tuple;
  }

  @Override
  public JsonNode internalToExternal(TupleValue tuple) {
    if (tuple == null) {
      return null;
    }
    ArrayNode root = objectMapper.createArrayNode();
    int size = definition.getComponentTypes().size();
    for (int i = 0; i < size; i++) {
      ConvertingCodec<JsonNode, Object> eltCodec = eltCodecs.get(i);
      Object o = tuple.get(i, eltCodec.getInternalJavaType());
      JsonNode element = eltCodec.internalToExternal(o);
      root.add(element);
    }
    return root;
  }
}
