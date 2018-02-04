/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static java.util.stream.Collectors.toList;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JsonNodeToLongCodec extends JsonNodeToNumberCodec<Long> {

  public JsonNodeToLongCodec(
      ThreadLocal<DecimalFormat> numberFormat,
      OverflowStrategy overflowStrategy,
      RoundingMode roundingMode,
      DateTimeFormatter temporalFormat,
      TimeUnit timeUnit,
      ZonedDateTime epoch,
      Map<String, Boolean> booleanWords,
      List<BigDecimal> booleanNumbers) {
    super(
        TypeCodec.bigint(),
        numberFormat,
        overflowStrategy,
        roundingMode,
        temporalFormat,
        timeUnit,
        epoch,
        booleanWords,
        booleanNumbers.stream().map(BigDecimal::longValueExact).collect(toList()));
  }

  @Override
  public Long convertFrom(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isLong()) {
      return node.longValue();
    }
    Number number;
    if (node.isNumber()) {
      number = node.numberValue();
    } else {
      number = parseNumber(node);
    }
    return narrowNumber(number, Long.class);
  }

  @Override
  public JsonNode convertTo(Long value) {
    return JsonNodeFactory.instance.numberNode(value);
  }
}
