/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.Duration;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

public class JsonNodeToDurationCodec extends ConvertingCodec<JsonNode, Duration> {

  public static final JsonNodeToDurationCodec INSTANCE = new JsonNodeToDurationCodec();

  private JsonNodeToDurationCodec() {
    super(duration(), JsonNode.class);
  }

  @Override
  public Duration convertFrom(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    String s = node.asText();
    if (s == null || s.isEmpty()) {
      return null;
    }
    return Duration.from(s);
  }

  @Override
  public JsonNode convertTo(Duration value) {
    if (value == null) {
      return null;
    }
    return JsonNodeFactory.instance.textNode(value.toString());
  }
}
