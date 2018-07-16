/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json;

import static com.datastax.dsbulk.commons.codecs.json.JsonCodecUtils.JSON_NODE_FACTORY;
import static java.util.stream.Collectors.toList;

import com.datastax.dsbulk.commons.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.commons.codecs.util.TemporalFormat;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.fasterxml.jackson.databind.JsonNode;
import io.netty.util.concurrent.FastThreadLocal;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JsonNodeToByteCodec extends JsonNodeToNumberCodec<Byte> {

  public JsonNodeToByteCodec(
      FastThreadLocal<NumberFormat> numberFormat,
      OverflowStrategy overflowStrategy,
      RoundingMode roundingMode,
      TemporalFormat temporalFormat,
      ZoneId timeZone,
      TimeUnit timeUnit,
      ZonedDateTime epoch,
      Map<String, Boolean> booleanStrings,
      List<BigDecimal> booleanNumbers,
      List<String> nullStrings) {
    super(
        TypeCodecs.TINYINT,
        numberFormat,
        overflowStrategy,
        roundingMode,
        temporalFormat,
        timeZone,
        timeUnit,
        epoch,
        booleanStrings,
        booleanNumbers.stream().map(BigDecimal::byteValueExact).collect(toList()),
        nullStrings);
  }

  @Override
  public Byte externalToInternal(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
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
    return narrowNumber(number, Byte.class);
  }

  @Override
  public JsonNode internalToExternal(Byte value) {
    return value == null ? null : JSON_NODE_FACTORY.numberNode(value);
  }
}
