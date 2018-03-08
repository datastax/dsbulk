/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToUDTCodec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;

public class StringToUDTCodec extends ConvertingCodec<String, UDTValue> {

  private final JsonNodeToUDTCodec jsonCodec;
  private final ObjectMapper objectMapper;
  private final List<String> nullWords;

  public StringToUDTCodec(
      JsonNodeToUDTCodec jsonCodec, ObjectMapper objectMapper, List<String> nullWords) {
    super(jsonCodec.getTargetCodec(), String.class);
    this.jsonCodec = jsonCodec;
    this.objectMapper = objectMapper;
    this.nullWords = nullWords;
  }

  @Override
  public UDTValue convertFrom(String s) {
    if (s == null || s.isEmpty() || nullWords.contains(s)) {
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
  public String convertTo(UDTValue udt) {
    if (udt == null) {
      return nullWords.isEmpty() ? null : nullWords.get(0);
    }
    try {
      JsonNode node = jsonCodec.convertTo(udt);
      return objectMapper.writeValueAsString(node);
    } catch (JsonProcessingException e) {
      throw new InvalidTypeException(String.format("Could not format '%s' to Json", udt), e);
    }
  }
}
