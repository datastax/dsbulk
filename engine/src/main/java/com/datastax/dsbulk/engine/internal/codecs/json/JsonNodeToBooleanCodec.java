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

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;

public class JsonNodeToBooleanCodec extends ConvertingCodec<JsonNode, Boolean> {

  private final Map<String, Boolean> inputs;
  private final List<String> nullWords;

  public JsonNodeToBooleanCodec(Map<String, Boolean> inputs, List<String> nullWords) {
    super(cboolean(), JsonNode.class);
    this.inputs = inputs;
    this.nullWords = nullWords;
  }

  @Override
  public Boolean convertFrom(JsonNode node) {
    if (node == null
        || node.isNull()
        || (node.isValueNode() && nullWords.contains(node.asText()))) {
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
      return JSON_NODE_FACTORY.nullNode();
    }
    return JSON_NODE_FACTORY.booleanNode(value);
  }
}
