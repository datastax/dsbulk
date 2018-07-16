/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json;

import static com.datastax.dsbulk.commons.codecs.json.JsonCodecUtils.JSON_NODE_FACTORY;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;

public class JsonNodeToBooleanCodec extends JsonNodeConvertingCodec<Boolean> {

  private final Map<String, Boolean> inputs;

  public JsonNodeToBooleanCodec(Map<String, Boolean> inputs, List<String> nullStrings) {
    super(TypeCodecs.BOOLEAN, nullStrings);
    this.inputs = inputs;
  }

  @Override
  public Boolean externalToInternal(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    if (node.isBoolean()) {
      return node.asBoolean();
    }
    String s = node.asText();
    Boolean b = inputs.get(s.toLowerCase());
    if (b == null) {
      throw new IllegalArgumentException("Invalid boolean value: " + s);
    }
    return b;
  }

  @Override
  public JsonNode internalToExternal(Boolean value) {
    return value == null ? null : JSON_NODE_FACTORY.booleanNode(value);
  }
}
