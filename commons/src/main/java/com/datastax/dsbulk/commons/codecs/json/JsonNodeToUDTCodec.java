/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JsonNodeToUDTCodec extends JsonNodeConvertingCodec<UdtValue> {

  private final Map<CqlIdentifier, ConvertingCodec<JsonNode, Object>> fieldCodecs;
  private final UserDefinedType definition;
  private final ObjectMapper objectMapper;

  public JsonNodeToUDTCodec(
      TypeCodec<UdtValue> udtCodec,
      Map<CqlIdentifier, ConvertingCodec<JsonNode, Object>> fieldCodecs,
      ObjectMapper objectMapper,
      List<String> nullStrings) {
    super(udtCodec, nullStrings);
    this.fieldCodecs = fieldCodecs;
    definition = (UserDefinedType) udtCodec.getCqlType();
    this.objectMapper = objectMapper;
  }

  @Override
  public UdtValue externalToInternal(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    if (!node.isObject()) {
      throw new IllegalArgumentException("Expecting OBJECT node, got " + node.getNodeType());
    }
    if (node.size() == 0) {
      return definition.newValue();
    }
    if (node.size() != definition.getFieldNames().size()) {
      throw new IllegalArgumentException(
          String.format(
              "Expecting %d fields, got %d", definition.getFieldNames().size(), node.size()));
    }
    UdtValue value = definition.newValue();
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    Collection<CqlIdentifier> fieldNames = definition.getFieldNames();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      CqlIdentifier name = CqlIdentifier.fromInternal(entry.getKey());
      if (!fieldNames.contains(name)) {
        throw new IllegalArgumentException(
            String.format("Unknown field %s in UDT %s", name, definition.getName()));
      }
      ConvertingCodec<JsonNode, Object> fieldCodec = fieldCodecs.get(name);
      Object o = fieldCodec.externalToInternal(entry.getValue());
      value = value.set(name, o, fieldCodec.getInternalJavaType());
    }
    return value;
  }

  @Override
  public JsonNode internalToExternal(UdtValue value) {
    if (value == null) {
      return null;
    }
    ObjectNode root = objectMapper.createObjectNode();
    for (CqlIdentifier name : definition.getFieldNames()) {
      ConvertingCodec<JsonNode, Object> eltCodec = fieldCodecs.get(name);
      Object o = value.get(name, eltCodec.getInternalJavaType());
      JsonNode node = eltCodec.internalToExternal(o);
      root.set(name.asInternal(), node);
    }
    return root;
  }
}
