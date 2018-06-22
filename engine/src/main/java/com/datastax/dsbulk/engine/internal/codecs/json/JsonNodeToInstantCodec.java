/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import com.datastax.dsbulk.engine.internal.codecs.util.TemporalFormat;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.fasterxml.jackson.databind.JsonNode;
import io.netty.util.concurrent.FastThreadLocal;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class JsonNodeToInstantCodec extends JsonNodeToTemporalCodec<Instant> {

  private final FastThreadLocal<NumberFormat> numberFormat;
  private final ZoneId timeZone;
  private final TimeUnit timeUnit;
  private final ZonedDateTime epoch;

  public JsonNodeToInstantCodec(
      TemporalFormat temporalFormat,
      FastThreadLocal<NumberFormat> numberFormat,
      ZoneId timeZone,
      TimeUnit timeUnit,
      ZonedDateTime epoch,
      List<String> nullStrings) {
    super(TypeCodecs.TIMESTAMP, temporalFormat, nullStrings);
    this.numberFormat = numberFormat;
    this.timeZone = timeZone;
    this.timeUnit = timeUnit;
    this.epoch = epoch;
  }

  @Override
  public Instant externalToInternal(JsonNode node) {
    TemporalAccessor temporal = parseTemporalAccessor(node);
    if (temporal == null) {
      return null;
    }
    return CodecUtils.convertTemporal(temporal, Instant.class, timeZone, epoch.toLocalDate());
  }

  @Override
  TemporalAccessor parseTemporalAccessor(JsonNode node) {
    if (isNullOrEmpty(node)) {
      return null;
    }
    String s = node.asText();
    // For timestamps, the conversion is more complex than for other temporals
    return CodecUtils.parseTemporal(
        s, temporalFormat, numberFormat.get(), timeUnit, epoch.toInstant());
  }
}
