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
