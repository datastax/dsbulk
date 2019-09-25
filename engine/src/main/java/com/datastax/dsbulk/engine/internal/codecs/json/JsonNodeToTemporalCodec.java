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
import com.datastax.dsbulk.engine.internal.codecs.util.NumericTemporalFormat;
import com.datastax.dsbulk.engine.internal.codecs.util.TemporalFormat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import java.math.BigDecimal;
import java.math.BigInteger;
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
    if (value == null) {
      return null;
    } else if (temporalFormat instanceof NumericTemporalFormat) {
      Number n = ((NumericTemporalFormat) temporalFormat).temporalToNumber(value);
      if (n == null) {
        return null;
      }
      if (n instanceof Byte) {
        return JSON_NODE_FACTORY.numberNode((Byte) n);
      }
      if (n instanceof Short) {
        return JSON_NODE_FACTORY.numberNode((Short) n);
      }
      if (n instanceof Integer) {
        return JSON_NODE_FACTORY.numberNode((Integer) n);
      }
      if (n instanceof Long) {
        return JSON_NODE_FACTORY.numberNode((Long) n);
      }
      if (n instanceof Float) {
        return JSON_NODE_FACTORY.numberNode((Float) n);
      }
      if (n instanceof Double) {
        return JSON_NODE_FACTORY.numberNode((Double) n);
      }
      if (n instanceof BigInteger) {
        return JSON_NODE_FACTORY.numberNode((BigInteger) n);
      }
      if (n instanceof BigDecimal) {
        return JSON_NODE_FACTORY.numberNode((BigDecimal) n);
      }
      return JSON_NODE_FACTORY.textNode(n.toString());
    } else {
      return JSON_NODE_FACTORY.textNode(temporalFormat.format(value));
    }
  }

  TemporalAccessor parseTemporalAccessor(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    if (node instanceof NumericNode && temporalFormat instanceof NumericTemporalFormat) {
      Number n = node.numberValue();
      return ((NumericTemporalFormat) temporalFormat).numberToTemporal(n);
    } else {
      String s = node.asText();
      return temporalFormat.parse(s);
    }
  }
}
