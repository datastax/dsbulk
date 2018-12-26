/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
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
      throw new InvalidTypeException("Expecting ARRAY node, got " + node.getNodeType());
    }
    int size = definition.getComponentTypes().size();
    if (node.size() > size && !allowExtraFields) {
      node.fieldNames();
      throw new InvalidTypeException(
          String.format(
              "JSON array does not match tuple definition: expecting %d elements, got %d "
                  + "(set schema.allowExtraFields to true to allow "
                  + "JSON arrays to contain more elements than the tuple definition).",
              size, node.size()));
    }
    if (node.size() < size && !allowMissingFields) {
      throw new InvalidTypeException(
          String.format(
              "JSON array does not match tuple definition: expecting %d elements, got %d "
                  + "(set schema.allowMissingFields to true to allow "
                  + "JSON arrays to contain fewer elements than the tuple definition).",
              size, node.size()));
    }
    TupleValue tuple = definition.newValue();
    for (int i = 0; i < size && i < node.size(); i++) {
      ConvertingCodec<JsonNode, Object> eltCodec = eltCodecs.get(i);
      Object o = eltCodec.externalToInternal(node.get(i));
      tuple.set(i, o, eltCodec.getInternalJavaType());
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
