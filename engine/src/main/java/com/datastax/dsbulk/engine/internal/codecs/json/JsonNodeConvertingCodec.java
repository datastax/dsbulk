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
import java.util.List;

abstract class JsonNodeConvertingCodec<T> extends ConvertingCodec<JsonNode, T> {

  private final List<String> nullWords;

  JsonNodeConvertingCodec(TypeCodec<T> targetCodec, List<String> nullWords) {
    super(targetCodec, JsonNode.class);
    this.nullWords = nullWords;
  }

  boolean isNull(JsonNode node) {
    return node == null
        || node.isNull()
        || (node.isValueNode() && nullWords.contains(node.asText()));
  }
}
