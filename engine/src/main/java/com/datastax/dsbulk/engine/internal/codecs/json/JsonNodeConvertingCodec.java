/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public abstract class JsonNodeConvertingCodec<T> extends ConvertingCodec<JsonNode, T> {

  private final List<String> nullStrings;

  protected JsonNodeConvertingCodec(TypeCodec<T> targetCodec, List<String> nullStrings) {
    super(targetCodec, JsonNode.class);
    this.nullStrings = nullStrings;
  }

  protected boolean isNull(JsonNode node) {
    return node == null
        || node.isNull()
        || (node.isValueNode() && nullStrings.contains(node.asText()));
  }

  protected boolean isNullOrEmpty(JsonNode node) {
    return isNull(node) || (node.isValueNode() && node.asText().isEmpty());
  }
}
