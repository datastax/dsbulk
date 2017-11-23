/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.extras.codecs.jdk8.LocalTimeCodec;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.DateTimeException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

public class JsonNodeToLocalTimeCodec extends JsonNodeToTemporalCodec<LocalTime> {

  public JsonNodeToLocalTimeCodec(DateTimeFormatter parser) {
    super(LocalTimeCodec.instance, parser);
  }

  @Override
  public LocalTime convertFrom(JsonNode s) {
    TemporalAccessor temporal = parseAsTemporalAccessor(s);
    if (temporal == null) {
      return null;
    }
    try {
      return LocalTime.from(temporal);
    } catch (DateTimeException e) {
      throw new InvalidTypeException("Cannot parse local time:" + s, e);
    }
  }
}
