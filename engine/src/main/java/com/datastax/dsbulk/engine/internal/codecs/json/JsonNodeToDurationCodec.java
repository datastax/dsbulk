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

import com.datastax.driver.core.Duration;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;

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
    return JSON_NODE_FACTORY.textNode(value.toString());
  }
}
