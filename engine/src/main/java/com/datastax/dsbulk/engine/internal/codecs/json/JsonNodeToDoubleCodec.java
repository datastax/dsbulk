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

public class JsonNodeToDoubleCodec extends JsonNodeToNumberCodec<Double> {

  public JsonNodeToDoubleCodec(
      ThreadLocal<DecimalFormat> formatter,
      DateTimeFormatter temporalParser,
      TimeUnit numericTimestampUnit,
      Instant numericTimestampEpoch) {
    super(cdouble(), formatter, temporalParser, numericTimestampUnit, numericTimestampEpoch);
  }

  @Override
  public Double convertFrom(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isDouble()) {
      return node.doubleValue();
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
    return CodecUtils.toDoubleValue(number);
  }

  @Override
  public JsonNode convertTo(Double value) {
    return JsonNodeFactory.instance.numberNode(value);
  }
}
