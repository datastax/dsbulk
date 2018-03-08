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
import static java.util.stream.Collectors.toList;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import com.fasterxml.jackson.databind.JsonNode;
import io.netty.util.concurrent.FastThreadLocal;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JsonNodeToFloatCodec extends JsonNodeToNumberCodec<Float> {

  public JsonNodeToFloatCodec(
      FastThreadLocal<NumberFormat> numberFormat,
      OverflowStrategy overflowStrategy,
      RoundingMode roundingMode,
      DateTimeFormatter temporalFormat,
      TimeUnit timeUnit,
      ZonedDateTime epoch,
      Map<String, Boolean> booleanWords,
      List<BigDecimal> booleanNumbers,
      List<String> nullWords) {
    super(
        TypeCodec.cfloat(),
        numberFormat,
        overflowStrategy,
        roundingMode,
        temporalFormat,
        timeUnit,
        epoch,
        booleanWords,
        booleanNumbers.stream().map(BigDecimal::floatValue).collect(toList()),
        nullWords);
  }

  @Override
  public Float externalToInternal(JsonNode node) {
    if (node == null
        || node.isNull()
        || (node.isValueNode() && nullWords.contains(node.asText()))) {
      return null;
    }
    if (node.isFloat()) {
      return node.floatValue();
    }
    Number number;
    if (node.isNumber()) {
      number = node.numberValue();
    } else {
      number = parseNumber(node);
    }
    if (number == null) {
      return null;
    }
    return narrowNumber(number, Float.class);
  }

  @Override
  public JsonNode internalToExternal(Float value) {
    return JSON_NODE_FACTORY.numberNode(value);
  }
}
