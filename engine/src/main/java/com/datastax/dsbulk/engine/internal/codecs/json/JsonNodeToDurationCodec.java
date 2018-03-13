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
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public class JsonNodeToDurationCodec extends JsonNodeConvertingCodec<Duration> {

  public JsonNodeToDurationCodec(List<String> nullWords) {
    super(duration(), nullWords);
  }

  @Override
  public Duration externalToInternal(JsonNode node) {
    if (isNull(node)) {
      return null;
    }
    String s = node.asText();
    if (s == null || s.isEmpty()) {
      return null;
    }
    return Duration.from(s);
  }

  @Override
  public JsonNode internalToExternal(Duration value) {
    if (value == null) {
      return null;
    }
    return JSON_NODE_FACTORY.textNode(value.toString());
  }
}
