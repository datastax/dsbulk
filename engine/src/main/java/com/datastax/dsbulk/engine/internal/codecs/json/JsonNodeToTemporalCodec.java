/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

public abstract class JsonNodeToTemporalCodec<T extends TemporalAccessor>
    extends ConvertingCodec<JsonNode, T> {

  final DateTimeFormatter parser;

  JsonNodeToTemporalCodec(TypeCodec<T> targetCodec, DateTimeFormatter parser) {
    super(targetCodec, JsonNode.class);
    this.parser = parser;
  }

  TemporalAccessor parseAsTemporalAccessor(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    String s = node.asText();
    return CodecUtils.parseTemporal(s, parser);
  }

  @Override
  public JsonNode convertTo(T value) {
    if (value == null) {
      return JsonNodeFactory.instance.nullNode();
    }
    return JsonNodeFactory.instance.textNode(parser.format(value));
  }
}
