/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.UUID;

public class JsonNodeToUUIDCodec extends ConvertingCodec<JsonNode, UUID> {

  public JsonNodeToUUIDCodec(TypeCodec<UUID> targetCodec) {
    super(targetCodec, JsonNode.class);
  }

  @Override
  public UUID convertFrom(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    String s = node.asText();
    try {
      return UUID.fromString(s);
    } catch (IllegalArgumentException e) {
      throw new InvalidTypeException("Invalid UUID String: " + s);
    }
  }

  @Override
  public JsonNode convertTo(UUID value) {
    if (value == null) {
      return null;
    }
    return JsonNodeFactory.instance.textNode(value.toString());
  }
}
