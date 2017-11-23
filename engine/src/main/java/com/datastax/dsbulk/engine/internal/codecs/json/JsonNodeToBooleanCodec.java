/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Map;

public class JsonNodeToBooleanCodec extends ConvertingCodec<JsonNode, Boolean> {

  private final Map<String, Boolean> inputs;

  public JsonNodeToBooleanCodec(Map<String, Boolean> inputs) {
    super(cboolean(), JsonNode.class);
    this.inputs = inputs;
  }

  @Override
  public Boolean convertFrom(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isBoolean()) {
      return node.asBoolean();
    }
    String s = node.asText();
    if (s == null || s.isEmpty()) {
      return null;
    }
    Boolean b = inputs.get(s.toLowerCase());
    if (b == null) {
      throw new InvalidTypeException("Invalid boolean value: " + s);
    }
    return b;
  }

  @Override
  public JsonNode convertTo(Boolean value) {
    if (value == null) {
      return JsonNodeFactory.instance.nullNode();
    }
    return JsonNodeFactory.instance.booleanNode(value);
  }
}
