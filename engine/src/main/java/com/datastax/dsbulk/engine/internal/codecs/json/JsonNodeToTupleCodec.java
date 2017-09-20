/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.List;

public class JsonNodeToTupleCodec extends ConvertingCodec<JsonNode, TupleValue> {

  private final TupleType definition;
  private final List<ConvertingCodec<JsonNode, Object>> eltCodecs;
  private final ObjectMapper objectMapper;

  public JsonNodeToTupleCodec(
      TypeCodec<TupleValue> tupleCodec,
      List<ConvertingCodec<JsonNode, Object>> eltCodecs,
      ObjectMapper objectMapper) {
    super(tupleCodec, JsonNode.class);
    this.eltCodecs = eltCodecs;
    definition = (TupleType) tupleCodec.getCqlType();
    this.objectMapper = objectMapper;
  }

  @Override
  public TupleValue convertFrom(JsonNode node) {
    if (node == null || node.isNull() || node.size() == 0) {
      return null;
    }
    TupleValue tuple = definition.newValue();
    for (int i = 0; i < definition.getComponentTypes().size(); i++) {
      ConvertingCodec<JsonNode, Object> eltCodec = eltCodecs.get(i);
      Object o = eltCodec.convertFrom(node.get(i));
      tuple.set(i, o, eltCodec.getTargetJavaType());
    }
    return tuple;
  }

  @Override
  public JsonNode convertTo(TupleValue tuple) {
    if (tuple == null) {
      return null;
    }
    ArrayNode root = objectMapper.createArrayNode();
    int size = definition.getComponentTypes().size();
    for (int i = 0; i < size; i++) {
      ConvertingCodec<JsonNode, Object> eltCodec = eltCodecs.get(i);
      Object o = tuple.get(i, eltCodec.getTargetJavaType());
      JsonNode element = eltCodec.convertTo(o);
      root.add(element);
    }
    return root;
  }
}
