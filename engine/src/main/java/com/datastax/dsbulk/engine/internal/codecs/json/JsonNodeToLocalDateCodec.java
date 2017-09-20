/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

public class JsonNodeToLocalDateCodec extends JsonNodeToTemporalCodec<LocalDate> {

  public JsonNodeToLocalDateCodec(DateTimeFormatter parser) {
    super(LocalDateCodec.instance, parser);
  }

  @Override
  public LocalDate convertFrom(JsonNode node) {
    TemporalAccessor temporal = parseAsTemporalAccessor(node);
    if (temporal == null) {
      return null;
    }
    try {
      return LocalDate.from(temporal);
    } catch (DateTimeException e) {
      throw new InvalidTypeException("Cannot parse local date:" + node, e);
    }
  }
}
