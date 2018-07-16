/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json.dse;

import static com.datastax.dsbulk.commons.codecs.json.JsonCodecUtils.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;

import com.datastax.dsbulk.commons.codecs.json.JsonCodecUtils;
import com.datastax.dse.driver.api.core.type.geometry.Point;
import com.datastax.dse.driver.internal.core.type.geometry.DefaultPoint;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonNodeToPointCodecTest {

  private List<String> nullStrings = newArrayList("NULL");
  private Point point = new DefaultPoint(-1.1, -2.2);
  private ObjectMapper objectMapper = JsonCodecUtils.getObjectMapper();
  private JsonNode geoJsonNode =
      objectMapper.readTree("{\"type\":\"Point\",\"coordinates\":[-1.1,-2.2]}");

  JsonNodeToPointCodecTest() throws IOException {}

  @Test
  void should_convert_from_valid_external() throws IOException {
    JsonNodeToPointCodec codec = new JsonNodeToPointCodec(objectMapper, nullStrings);
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("'POINT (-1.1 -2.2)'"))
        .toInternal(point)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(" point (-1.1 -2.2) "))
        .toInternal(point)
        .convertsFromExternal(
            JSON_NODE_FACTORY.textNode(objectMapper.writeValueAsString(geoJsonNode)))
        .toInternal(point)
        .convertsFromExternal(geoJsonNode)
        .toInternal(point)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    JsonNodeToPointCodec codec = new JsonNodeToPointCodec(objectMapper, nullStrings);
    assertThat(codec).convertsFromInternal(point).toExternal(geoJsonNode);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    JsonNodeToPointCodec codec = new JsonNodeToPointCodec(objectMapper, nullStrings);
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid point literal"));
  }
}
