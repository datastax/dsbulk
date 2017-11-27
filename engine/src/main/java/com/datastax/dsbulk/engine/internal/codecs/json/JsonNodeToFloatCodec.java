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

public class JsonNodeToFloatCodec extends JsonNodeToNumberCodec<Float> {

  public JsonNodeToFloatCodec(
      ThreadLocal<DecimalFormat> formatter,
      DateTimeFormatter temporalParser,
      TimeUnit numericTimestampUnit,
      Instant numericTimestampEpoch) {
    super(cfloat(), formatter, temporalParser, numericTimestampUnit, numericTimestampEpoch);
  }

  @Override
  public Float convertFrom(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isFloat()) {
      return node.floatValue();
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
    return CodecUtils.toFloatValue(number);
  }

  @Override
  public JsonNode convertTo(Float value) {
    return JsonNodeFactory.instance.numberNode(value);
  }
}
