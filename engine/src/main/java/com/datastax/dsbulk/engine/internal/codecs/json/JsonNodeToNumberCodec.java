/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

abstract class JsonNodeToNumberCodec<N extends Number> extends ConvertingCodec<JsonNode, N> {

  private final ThreadLocal<DecimalFormat> formatter;
  final DateTimeFormatter temporalParser;
  final TimeUnit numericTimestampUnit;
  final Instant numericTimestampEpoch;

  JsonNodeToNumberCodec(
      TypeCodec<N> targetCodec,
      ThreadLocal<DecimalFormat> formatter,
      DateTimeFormatter temporalParser,
      TimeUnit numericTimestampUnit,
      Instant numericTimestampEpoch) {
    super(targetCodec, JsonNode.class);
    this.formatter = formatter;
    this.temporalParser = temporalParser;
    this.numericTimestampUnit = numericTimestampUnit;
    this.numericTimestampEpoch = numericTimestampEpoch;
  }

  DecimalFormat getNumberFormat() {
    DecimalFormat format = formatter.get();
    format.setParseBigDecimal(true);
    return format;
  }
}
