/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class JsonNodeToIntegerCodec extends JsonNodeToNumberCodec<Integer> {

  public JsonNodeToIntegerCodec(
      ThreadLocal<DecimalFormat> formatter,
      DateTimeFormatter temporalParser,
      TimeUnit numericTimestampUnit,
      Instant numericTimestampEpoch) {
    super(cint(), formatter, temporalParser, numericTimestampUnit, numericTimestampEpoch);
  }

  @Override
  public Integer convertFrom(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isInt()) {
      return node.intValue();
    }
    Number number =
        CodecUtils.parseNumber(
            node.asText(),
            getNumberFormat(),
            temporalParser,
            numericTimestampUnit,
            numericTimestampEpoch);
    if (number == null) {
      return null;
    }
    return CodecUtils.toIntValueExact(number);
  }

  @Override
  public JsonNode convertTo(Integer value) {
    return JsonNodeFactory.instance.numberNode(value);
  }
}
