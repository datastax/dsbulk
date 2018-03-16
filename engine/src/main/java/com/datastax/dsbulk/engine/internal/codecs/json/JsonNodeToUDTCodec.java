/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JsonNodeToUDTCodec extends JsonNodeConvertingCodec<UDTValue> {

  private final Map<String, ConvertingCodec<JsonNode, Object>> fieldCodecs;
  private final UserType definition;
  private final ObjectMapper objectMapper;

  public JsonNodeToUDTCodec(
      TypeCodec<UDTValue> udtCodec,
      Map<String, ConvertingCodec<JsonNode, Object>> fieldCodecs,
      ObjectMapper objectMapper,
      List<String> nullStrings) {
    super(udtCodec, nullStrings);
    this.fieldCodecs = fieldCodecs;
    definition = (UserType) udtCodec.getCqlType();
    this.objectMapper = objectMapper;
  }

  @Override
  public UDTValue externalToInternal(JsonNode node) {
    if (isNull(node)) {
      return null;
    }
    if (!node.isObject()) {
      throw new InvalidTypeException("Expecting OBJECT node, got " + node.getNodeType());
    }
    if (node.size() == 0) {
      return null;
    }
    if (node.size() != definition.size()) {
      throw new InvalidTypeException(
          String.format("Expecting %d fields, got %d", definition.size(), node.size()));
    }
    UDTValue value = definition.newValue();
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    Collection<String> fieldNames = definition.getFieldNames();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      String name = entry.getKey();
      if (!fieldNames.contains(name)) {
        throw new InvalidTypeException(
            String.format("Unknown field %s in UDT %s", name, definition.getName()));
      }
      ConvertingCodec<JsonNode, Object> fieldCodec = fieldCodecs.get(name);
      Object o = fieldCodec.externalToInternal(entry.getValue());
      value.set(name, o, fieldCodec.getInternalJavaType());
    }
    return value;
  }

  @Override
  public JsonNode internalToExternal(UDTValue value) {
    if (value == null) {
      return objectMapper.getNodeFactory().nullNode();
    }
    ObjectNode root = objectMapper.createObjectNode();
    for (UserType.Field field : definition) {
      String name = field.getName();
      ConvertingCodec<JsonNode, Object> eltCodec = fieldCodecs.get(name);
      Object o = value.get(name, eltCodec.getInternalJavaType());
      JsonNode node = eltCodec.internalToExternal(o);
      root.set(name, node);
    }
    return root;
  }
}
