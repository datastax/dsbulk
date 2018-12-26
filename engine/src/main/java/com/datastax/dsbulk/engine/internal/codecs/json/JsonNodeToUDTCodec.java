/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.UserType.Field;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JsonNodeToUDTCodec extends JsonNodeConvertingCodec<UDTValue> {

  private final Map<String, ConvertingCodec<JsonNode, Object>> fieldCodecs;
  private final UserType definition;
  private final ObjectMapper objectMapper;
  private final boolean allowExtraFields;
  private final boolean allowMissingFields;

  public JsonNodeToUDTCodec(
      TypeCodec<UDTValue> udtCodec,
      Map<String, ConvertingCodec<JsonNode, Object>> fieldCodecs,
      ObjectMapper objectMapper,
      List<String> nullStrings,
      boolean allowExtraFields,
      boolean allowMissingFields) {
    super(udtCodec, nullStrings);
    this.fieldCodecs = fieldCodecs;
    definition = (UserType) udtCodec.getCqlType();
    this.objectMapper = objectMapper;
    this.allowExtraFields = allowExtraFields;
    this.allowMissingFields = allowMissingFields;
  }

  @Override
  public UDTValue externalToInternal(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    if (!(node.isObject() || node.isArray())) {
      throw new InvalidTypeException("Expecting OBJECT or ARRAY node, got " + node.getNodeType());
    }
    if (node.size() == 0 && allowMissingFields) {
      return definition.newValue();
    }
    UDTValue value = definition.newValue();
    if (node.isObject()) {
      checkJsonObject(node);
      for (Field field : definition) {
        String name = field.getName();
        if (node.has(name)) {
          ConvertingCodec<JsonNode, Object> fieldCodec = fieldCodecs.get(name);
          Object o = fieldCodec.externalToInternal(node.get(name));
          value.set(quoteIfNecessary(name), o, fieldCodec.getInternalJavaType());
        }
      }
    } else {
      checkJsonArray(node);
      // The field iteration order is deterministic
      Iterator<Field> fields = definition.iterator();
      for (int i = 0; i < node.size() && fields.hasNext(); i++) {
        JsonNode element = node.get(i);
        Field field = fields.next();
        ConvertingCodec<JsonNode, Object> fieldCodec = fieldCodecs.get(field.getName());
        Object o = fieldCodec.externalToInternal(element);
        value.set(i, o, fieldCodec.getInternalJavaType());
      }
    }
    return value;
  }

  private void checkJsonObject(JsonNode node) {
    Set<String> udtFieldNames = new LinkedHashSet<>(definition.getFieldNames());
    Set<String> nodeFieldNames = new LinkedHashSet<>();
    Iterators.addAll(nodeFieldNames, node.fieldNames());
    if (!udtFieldNames.equals(nodeFieldNames)) {
      Set<String> extraneous = Sets.difference(nodeFieldNames, udtFieldNames);
      Set<String> missing = Sets.difference(udtFieldNames, nodeFieldNames);
      boolean hasExtras = !allowExtraFields && !extraneous.isEmpty();
      boolean hasMissing = !allowMissingFields && !missing.isEmpty();
      if (hasMissing || hasExtras) {
        StringBuilder msg = new StringBuilder("JSON object does not match UDT definition: ");
        if (hasExtras) {
          msg.append("found ").append(extraneous.size()).append(" extraneous field");
          if (extraneous.size() > 1) {
            msg.append('s');
          }
          msg.append(": '").append(String.join("', '", extraneous)).append("'");
        }
        if (hasMissing) {
          if (hasExtras) {
            msg.append(" and ");
          }
          msg.append(missing.size());
          if (missing.size() > 1) {
            msg.append(" fields are missing: ");
          } else {
            msg.append(" field is missing: ");
          }
          msg.append("'").append(String.join("', '", missing)).append("'");
        }
        msg.append(" (");
        if (hasExtras) {
          msg.append("set schema.allowExtraFields to true to allow ")
              .append("JSON objects to contain fields not present in the UDT definition");
        }
        if (hasMissing) {
          if (hasExtras) {
            msg.append(" and ");
          }
          msg.append("set schema.allowMissingFields to true to allow ")
              .append("JSON objects to lack of fields present in the UDT definition");
        }
        msg.append(").");
        throw new InvalidTypeException(msg.toString());
      }
    }
  }

  private void checkJsonArray(JsonNode node) {
    if (node.size() > definition.size() && !allowExtraFields) {
      throw new InvalidTypeException(
          String.format(
              "JSON array does not match UDT definition: expecting %d elements, got %d "
                  + "(set schema.allowExtraFields to true to allow "
                  + "JSON arrays to contain more elements than the UDT definition).",
              definition.size(), node.size()));
    }
    if (node.size() < definition.size() && !allowMissingFields) {
      throw new InvalidTypeException(
          String.format(
              "JSON array does not match UDT definition: expecting %d elements, got %d "
                  + "(set schema.allowMissingFields to true to allow "
                  + "JSON arrays to contain fewer elements than the UDT definition).",
              definition.size(), node.size()));
    }
  }

  @Override
  public JsonNode internalToExternal(UDTValue value) {
    if (value == null) {
      return null;
    }
    ObjectNode root = objectMapper.createObjectNode();
    for (UserType.Field field : definition) {
      String name = field.getName();
      ConvertingCodec<JsonNode, Object> eltCodec = fieldCodecs.get(name);
      Object o = value.get(quoteIfNecessary(name), eltCodec.getInternalJavaType());
      JsonNode node = eltCodec.internalToExternal(o);
      root.set(name, node);
    }
    return root;
  }
}
