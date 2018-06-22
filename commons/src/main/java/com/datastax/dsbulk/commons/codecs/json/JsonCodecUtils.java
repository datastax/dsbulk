/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.math.BigDecimal;

public class JsonCodecUtils {
  /**
   * A {@link JsonNodeFactory} that preserves {@link BigDecimal} scales, used to generate Json
   * nodes.
   */
  public static final JsonNodeFactory JSON_NODE_FACTORY =
      JsonNodeFactory.withExactBigDecimals(true);

  /**
   * The object mapper to use for converting Json nodes to and from Java types in Json codecs.
   *
   * <p>This is not the object mapper used by the Json connector to read and write Json files.
   *
   * @return The object mapper to use for converting Json nodes to and from Java types in Json
   *     codecs.
   */
  public static ObjectMapper getObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.setNodeFactory(JSON_NODE_FACTORY);
    // create a somewhat lenient mapper that recognizes a slightly relaxed Json syntax when parsing
    objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    objectMapper.configure(JsonParser.Feature.ALLOW_MISSING_VALUES, true);
    objectMapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    return objectMapper;
  }
}
