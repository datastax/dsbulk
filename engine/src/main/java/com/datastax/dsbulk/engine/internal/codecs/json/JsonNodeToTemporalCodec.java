/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.text.ParseException;
import java.text.ParsePosition;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;

public abstract class JsonNodeToTemporalCodec<T extends TemporalAccessor>
    extends ConvertingCodec<JsonNode, T> {

  protected final DateTimeFormatter parser;

  public JsonNodeToTemporalCodec(TypeCodec<T> targetCodec, DateTimeFormatter parser) {
    super(targetCodec, JsonNode.class);
    this.parser = parser;
  }

  protected TemporalAccessor parseAsTemporalAccessor(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.canConvertToLong()) {
      return Instant.ofEpochMilli(node.asLong());
    }
    String s = node.asText();
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      ParsePosition pos = new ParsePosition(0);
      TemporalAccessor accessor = parser.parse(s, pos);
      if (pos.getIndex() != s.length()) {
        throw new InvalidTypeException(
            "Cannot parse temporal: " + s, new ParseException(s, pos.getErrorIndex()));
      }
      return accessor;
    } catch (DateTimeParseException e) {
      throw new InvalidTypeException("Cannot parse temporal: " + node, e);
    }
  }

  @Override
  public JsonNode convertTo(T value) {
    if (value == null) {
      return null;
    }
    return JsonNodeFactory.instance.textNode(parser.format(value));
  }
}
