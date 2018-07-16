/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.string;

import com.datastax.dsbulk.commons.codecs.json.JsonNodeToUDTCodec;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;

public class StringToUDTCodec extends StringConvertingCodec<UdtValue> {

  private final JsonNodeToUDTCodec jsonCodec;
  private final ObjectMapper objectMapper;

  public StringToUDTCodec(
      JsonNodeToUDTCodec jsonCodec, ObjectMapper objectMapper, List<String> nullStrings) {
    super(jsonCodec.getInternalCodec(), nullStrings);
    this.jsonCodec = jsonCodec;
    this.objectMapper = objectMapper;
  }

  @Override
  public UdtValue externalToInternal(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    try {
      JsonNode node = objectMapper.readTree(s);
      return jsonCodec.externalToInternal(node);
    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("Could not parse '%s' as Json", s), e);
    }
  }

  @Override
  public String internalToExternal(UdtValue udt) {
    if (udt == null) {
      return nullString();
    }
    try {
      JsonNode node = jsonCodec.internalToExternal(udt);
      return objectMapper.writeValueAsString(node);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(String.format("Could not format '%s' to Json", udt), e);
    }
  }
}
