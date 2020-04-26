/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.codecs.text.json;

import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.math.BigDecimal;

public class JsonCodecUtils {

  public static final GenericType<JsonNode> JSON_NODE_TYPE = GenericType.of(JsonNode.class);

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
    return JsonMapper.builder()
        .nodeFactory(JSON_NODE_FACTORY)
        // create a somewhat lenient mapper that recognizes a slightly relaxed Json syntax when
        // parsing
        .enable(JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES)
        .enable(JsonReadFeature.ALLOW_MISSING_VALUES)
        .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
        .enable(JsonReadFeature.ALLOW_SINGLE_QUOTES)
        // fail on trailing tokens: the entire input must be parsed
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
        .build();
  }
}
