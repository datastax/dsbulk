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
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JsonNodeToBigDecimalCodec extends JsonNodeToNumberCodec<BigDecimal> {

  public JsonNodeToBigDecimalCodec(
      ThreadLocal<DecimalFormat> formatter,
      DateTimeFormatter temporalParser,
      TimeUnit numericTimestampUnit,
      Instant numericTimestampEpoch,
      Map<String, Boolean> booleanWords,
      List<BigDecimal> booleanNumbers) {
    super(
        decimal(),
        formatter,
        temporalParser,
        numericTimestampUnit,
        numericTimestampEpoch,
        booleanWords,
        booleanNumbers);
  }

  @Override
  public BigDecimal convertFrom(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isNumber()) {
      return node.decimalValue();
    }
    Number number = parseNumber(node);
    if (number == null) {
      return null;
    }
    return CodecUtils.toBigDecimal(number);
  }

  @Override
  public JsonNode convertTo(BigDecimal value) {
    return JsonNodeFactory.instance.numberNode(value);
  }
}
