/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Iterator;
import java.util.Map;

public class JsonNodeToUDTCodec extends ConvertingCodec<JsonNode, UDTValue> {

  private final Map<String, ConvertingCodec<JsonNode, Object>> fieldCodecs;
  private final UserType definition;
  private final ObjectMapper objectMapper;

  public JsonNodeToUDTCodec(
      TypeCodec<UDTValue> udtCodec,
      Map<String, ConvertingCodec<JsonNode, Object>> fieldCodecs,
      ObjectMapper objectMapper) {
    super(udtCodec, JsonNode.class);
    this.fieldCodecs = fieldCodecs;
    definition = (UserType) udtCodec.getCqlType();
    this.objectMapper = objectMapper;
  }

  @Override
  public UDTValue convertFrom(JsonNode node) {
    if (node == null || node.isNull() || node.size() == 0) {
      return null;
    }
    UDTValue value = definition.newValue();
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      String name = entry.getKey();
      if (definition.getFieldType(name) == null) {
        throw new InvalidTypeException(
            String.format("Unknown field %s in UDT %s", name, definition.getName()));
      }
      ConvertingCodec<JsonNode, Object> fieldCodec = fieldCodecs.get(name);
      Object o = fieldCodec.convertFrom(entry.getValue());
      value.set(name, o, fieldCodec.getTargetJavaType());
    }
    return value;
  }

  @Override
  public JsonNode convertTo(UDTValue value) {
    if (value == null) {
      return null;
    }
    ObjectNode root = objectMapper.createObjectNode();
    for (UserType.Field field : definition) {
      String name = field.getName();
      ConvertingCodec<JsonNode, Object> eltCodec = fieldCodecs.get(name);
      Object o = value.get(name, eltCodec.getTargetJavaType());
      JsonNode node = eltCodec.convertTo(o);
      root.set(name, node);
    }
    return root;
  }
}
