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
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

abstract class JsonNodeToNumberCodec<N extends Number> extends ConvertingCodec<JsonNode, N> {

  private final ThreadLocal<DecimalFormat> numberFormat;
  private final OverflowStrategy overflowStrategy;
  private final RoundingMode roundingMode;
  private final DateTimeFormatter temporalFormat;
  private final TimeUnit timeUnit;
  private final ZonedDateTime epoch;
  private final Map<String, Boolean> booleanWords;
  private final List<N> booleanNumbers;

  JsonNodeToNumberCodec(
      TypeCodec<N> targetCodec,
      ThreadLocal<DecimalFormat> numberFormat,
      OverflowStrategy overflowStrategy,
      RoundingMode roundingMode,
      DateTimeFormatter temporalFormat,
      TimeUnit timeUnit,
      ZonedDateTime epoch,
      Map<String, Boolean> booleanWords,
      List<N> booleanNumbers) {
    super(targetCodec, JsonNode.class);
    this.numberFormat = numberFormat;
    this.overflowStrategy = overflowStrategy;
    this.roundingMode = roundingMode;
    this.temporalFormat = temporalFormat;
    this.timeUnit = timeUnit;
    this.epoch = epoch;
    this.booleanWords = booleanWords;
    this.booleanNumbers = booleanNumbers;
  }

  Number parseNumber(JsonNode node) {
    return CodecUtils.parseNumber(
        node.asText(),
        numberFormat.get(),
        temporalFormat,
        timeUnit,
        epoch,
        booleanWords,
        booleanNumbers);
  }

  N narrowNumber(Number number, Class<? extends N> targetClass) {
    return CodecUtils.narrowNumber(number, targetClass, overflowStrategy, roundingMode);
  }
}
