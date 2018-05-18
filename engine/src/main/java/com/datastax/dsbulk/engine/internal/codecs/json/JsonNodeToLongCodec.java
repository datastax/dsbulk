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

public class JsonNodeToLongCodec extends JsonNodeToNumberCodec<Long> {

  public JsonNodeToLongCodec(
      FastThreadLocal<NumberFormat> numberFormat,
      OverflowStrategy overflowStrategy,
      RoundingMode roundingMode,
      DateTimeFormatter temporalFormat,
      TimeUnit timeUnit,
      ZonedDateTime epoch,
      Map<String, Boolean> booleanStrings,
      List<BigDecimal> booleanNumbers,
      List<String> nullStrings) {
    super(
        TypeCodec.bigint(),
        numberFormat,
        overflowStrategy,
        roundingMode,
        temporalFormat,
        timeUnit,
        epoch,
        booleanStrings,
        booleanNumbers.stream().map(BigDecimal::longValueExact).collect(toList()),
        nullStrings);
  }

  @Override
  public Long externalToInternal(JsonNode node) {
    if (isNull(node)) {
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
  public JsonNode internalToExternal(Long value) {
    return value == null ? null : JSON_NODE_FACTORY.numberNode(value);
  }
}
