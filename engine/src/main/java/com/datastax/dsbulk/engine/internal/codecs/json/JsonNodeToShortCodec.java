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

public class JsonNodeToShortCodec extends JsonNodeToNumberCodec<Short> {

  public JsonNodeToShortCodec(
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
        smallInt(),
        numberFormat,
        overflowStrategy,
        roundingMode,
        temporalFormat,
        timeUnit,
        epoch,
        booleanWords,
        booleanNumbers.stream().map(BigDecimal::shortValueExact).collect(toList()),
        nullWords);
  }

  @Override
  public Short externalToInternal(JsonNode node) {
    if (node == null
        || node.isNull()
        || (node.isValueNode() && nullWords.contains(node.asText()))) {
      return null;
    }
    if (node.isShort()) {
      return node.shortValue();
    }
    Number number;
    if (node.isNumber()) {
      number = node.numberValue();
    } else {
      number = parseNumber(node);
    }
    return narrowNumber(number, Short.class);
  }

  @Override
  public JsonNode internalToExternal(Short value) {
    return JSON_NODE_FACTORY.numberNode(value);
  }
}
