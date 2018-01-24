/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToTupleCodec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public class StringToTupleCodec extends ConvertingCodec<String, TupleValue> {

  private final JsonNodeToTupleCodec jsonCodec;
  private final ObjectMapper objectMapper;

  public StringToTupleCodec(JsonNodeToTupleCodec jsonCodec, ObjectMapper objectMapper) {
    super(jsonCodec.getTargetCodec(), String.class);
    this.jsonCodec = jsonCodec;
    this.objectMapper = objectMapper;
  }

  @Override
  public TupleValue convertFrom(String s) {
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
  public String convertTo(TupleValue tuple) {
    if (tuple == null) {
      return null;
    }
    try {
      JsonNode node = jsonCodec.convertTo(tuple);
      return objectMapper.writeValueAsString(node);
    } catch (JsonProcessingException e) {
      throw new InvalidTypeException(String.format("Could not format '%s' to Json", tuple), e);
    }
  }
}
