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
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.List;

public abstract class JsonNodeToTemporalCodec<T extends TemporalAccessor>
    extends ConvertingCodec<JsonNode, T> {

  final DateTimeFormatter temporalFormat;
  final List<String> nullWords;

  JsonNodeToTemporalCodec(
      TypeCodec<T> targetCodec, DateTimeFormatter temporalFormat, List<String> nullWords) {
    super(targetCodec, JsonNode.class);
    this.temporalFormat = temporalFormat;
    this.nullWords = nullWords;
  }

  @Override
  public JsonNode convertTo(T value) {
    if (value == null) {
      return JSON_NODE_FACTORY.nullNode();
    }
    return JSON_NODE_FACTORY.textNode(temporalFormat.format(value));
  }

  TemporalAccessor parseTemporalAccessor(JsonNode node) {
    if (node == null
        || node.isNull()
        || (node.isValueNode() && nullWords.contains(node.asText()))) {
      return null;
    }
    String s = node.asText();
    return CodecUtils.parseTemporal(s, temporalFormat);
  }
}
