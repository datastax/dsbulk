/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToCollectionCodec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collection;

public abstract class StringToCollectionCodec<E, C extends Collection<E>>
    extends ConvertingCodec<String, C> {

  private final JsonNodeToCollectionCodec<E, C> jsonCodec;
  private final ObjectMapper objectMapper;

  StringToCollectionCodec(JsonNodeToCollectionCodec<E, C> jsonCodec, ObjectMapper objectMapper) {
    super(jsonCodec.getTargetCodec(), String.class);
    this.jsonCodec = jsonCodec;
    this.objectMapper = objectMapper;
  }

  @Override
  public C convertFrom(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      JsonNode node = objectMapper.readTree(s);
      return jsonCodec.convertFrom(node);
    } catch (IOException e) {
      throw new InvalidTypeException(String.format("Could not parse '%s' as Json", s), e);
    }
  }

  @Override
  public String convertTo(C collection) {
    if (collection == null) {
      return null;
    }
    try {
      JsonNode node = jsonCodec.convertTo(collection);
      return objectMapper.writeValueAsString(node);
    } catch (JsonProcessingException e) {
      throw new InvalidTypeException(String.format("Could not format '%s' to Json", collection), e);
    }
  }
}
