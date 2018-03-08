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
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public class JsonNodeToStringCodec extends ConvertingCodec<JsonNode, String> {

  private final List<String> nullWords;

  public JsonNodeToStringCodec(TypeCodec<String> innerCodec, List<String> nullWords) {
    super(innerCodec, JsonNode.class);
    this.nullWords = nullWords;
  }

  @Override
  public String convertFrom(JsonNode node) {
    if (node == null
        || node.isNull()
        || (node.isValueNode() && nullWords.contains(node.asText()))) {
      return null;
    }
    return node.asText();
  }

  @Override
  public JsonNode convertTo(String value) {
    if (value == null) {
      return null;
    }
    return JSON_NODE_FACTORY.textNode(value);
  }
}
