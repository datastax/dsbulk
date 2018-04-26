/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;

public class JsonNodeToStringCodec extends JsonNodeConvertingCodec<String> {

  private final ObjectMapper objectMapper;

  public JsonNodeToStringCodec(
      TypeCodec<String> innerCodec, ObjectMapper objectMapper, List<String> nullStrings) {
    super(innerCodec, nullStrings);
    this.objectMapper = objectMapper;
  }

  @Override
  public String externalToInternal(JsonNode node) {
    if (isNull(node)) {
      return null;
    }
    if (node.isContainerNode()) {
      try {
        return objectMapper.writeValueAsString(node);
      } catch (JsonProcessingException e) {
        throw new InvalidTypeException("Cannot deserialize node " + node, e);
      }
    } else {
      return node.asText();
    }
  }

  @Override
  public JsonNode internalToExternal(String value) {
    if (value == null) {
      return JSON_NODE_FACTORY.nullNode();
    }
    try {
      // Try to read a valid json first (object, array, or numeric values),
      // and fall back to a textual node if that fails.
      JsonNode node = objectMapper.readTree(value);
      // readTree() may mess with the original value if it's not a valid json object, array or
      // number, so check that now. It may also mess with quoted strings, so exclude textual nodes
      // as well.
      if (node != null && !node.isNull() && !node.isTextual()) {
        return node;
      }
    } catch (IOException ignored) {
      // not a valid json at all
    }
    // As a last resort, return the value as a textual node
    return JSON_NODE_FACTORY.textNode(value);
  }
}
