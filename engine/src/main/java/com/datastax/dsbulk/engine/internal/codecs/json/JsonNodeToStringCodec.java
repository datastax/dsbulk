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
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    // DAT-297: do not convert empty strings to null so do not use isNullOrEmpty() here
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
      return null;
    }
    return objectMapper.getNodeFactory().textNode(value);
  }
}
