/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

public class JsonNodeToStringCodec extends ConvertingCodec<JsonNode, String> {

  public JsonNodeToStringCodec(TypeCodec<String> innerCodec) {
    super(innerCodec, JsonNode.class);
  }

  @Override
  public String convertFrom(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    return node.asText();
  }

  @Override
  public JsonNode convertTo(String value) {
    if (value == null) {
      return null;
    }
    return JsonNodeFactory.instance.textNode(value);
  }
}
