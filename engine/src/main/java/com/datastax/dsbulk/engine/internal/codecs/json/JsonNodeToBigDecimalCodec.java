/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.netty.util.concurrent.FastThreadLocal;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JsonNodeToBigDecimalCodec extends JsonNodeToNumberCodec<BigDecimal> {

  public JsonNodeToBigDecimalCodec(
      FastThreadLocal<NumberFormat> numberFormat,
      OverflowStrategy overflowStrategy,
      RoundingMode roundingMode,
      DateTimeFormatter temporalFormat,
      TimeUnit timeUnit,
      ZonedDateTime epoch,
      Map<String, Boolean> booleanWords,
      List<BigDecimal> booleanNumbers) {
    super(
        TypeCodec.decimal(),
        numberFormat,
        overflowStrategy,
        roundingMode,
        temporalFormat,
        timeUnit,
        epoch,
        booleanWords,
        booleanNumbers);
  }

  @Override
  public BigDecimal convertFrom(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isBigDecimal()) {
      return node.decimalValue();
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
    return CodecUtils.toBigDecimal(number);
  }

  @Override
  public JsonNode convertTo(BigDecimal value) {
    return JsonNodeFactory.instance.numberNode(value);
  }
}
