/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static java.util.stream.Collectors.toList;

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

public class JsonNodeToByteCodec extends JsonNodeToNumberCodec<Byte> {

  public JsonNodeToByteCodec(
      ThreadLocal<DecimalFormat> formatter,
      DateTimeFormatter temporalParser,
      TimeUnit numericTimestampUnit,
      Instant numericTimestampEpoch,
      Map<String, Boolean> booleanWords,
      List<BigDecimal> booleanNumbers) {
    super(
        tinyInt(),
        formatter,
        temporalParser,
        numericTimestampUnit,
        numericTimestampEpoch,
        booleanWords,
        booleanNumbers.stream().map(BigDecimal::byteValueExact).collect(toList()));
  }

  @Override
  public Byte convertFrom(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isInt()) {
      int i = node.intValue();
      byte b = (byte) i;
      if (i != b) {
        throw new java.lang.ArithmeticException("Overflow");
      }
      return b;
    }
    Number number = parseNumber(node);
    if (number == null) {
      return null;
    }
    return CodecUtils.toByteValueExact(number);
  }

  @Override
  public JsonNode convertTo(Byte value) {
    return JsonNodeFactory.instance.numberNode(value);
  }
}
