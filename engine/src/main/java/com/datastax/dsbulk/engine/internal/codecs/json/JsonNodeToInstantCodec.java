/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.concurrent.TimeUnit;

public class JsonNodeToInstantCodec extends JsonNodeToTemporalCodec<Instant> {

  private final TimeUnit numericTimestampUnit;
  private final Instant numericTimestampEpoch;

  public JsonNodeToInstantCodec(
      DateTimeFormatter parser, TimeUnit numericTimestampUnit, Instant numericTimestampEpoch) {
    super(InstantCodec.instance, parser);
    this.numericTimestampUnit = numericTimestampUnit;
    this.numericTimestampEpoch = numericTimestampEpoch;
  }

  @Override
  public Instant convertFrom(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    String s = node.asText();
    TemporalAccessor temporal =
        CodecUtils.parseTemporal(s, parser, numericTimestampUnit, numericTimestampEpoch);
    if (temporal == null) {
      return null;
    }
    try {
      return Instant.from(temporal);
    } catch (DateTimeException e) {
      throw new InvalidTypeException("Cannot parse instant:" + node, e);
    }
  }
}
