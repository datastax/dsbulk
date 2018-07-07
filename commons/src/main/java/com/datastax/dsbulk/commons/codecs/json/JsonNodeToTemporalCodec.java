/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json;

import static com.datastax.dsbulk.commons.config.CodecSettings.JSON_NODE_FACTORY;

import com.datastax.dsbulk.commons.codecs.util.TemporalFormat;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.temporal.TemporalAccessor;
import java.util.List;

public abstract class JsonNodeToTemporalCodec<T extends TemporalAccessor>
    extends JsonNodeConvertingCodec<T> {

  final TemporalFormat temporalFormat;

  JsonNodeToTemporalCodec(
      TypeCodec<T> targetCodec, TemporalFormat temporalFormat, List<String> nullStrings) {
    super(targetCodec, nullStrings);
    this.temporalFormat = temporalFormat;
  }

  @Override
  public JsonNode internalToExternal(T value) {
    return value == null ? null : JSON_NODE_FACTORY.textNode(temporalFormat.format(value));
  }

  TemporalAccessor parseTemporalAccessor(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    String s = node.asText();
    return temporalFormat.parse(s);
  }
}
