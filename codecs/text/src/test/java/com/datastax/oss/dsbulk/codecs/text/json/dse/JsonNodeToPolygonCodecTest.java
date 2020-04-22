/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs.text.json.dse;

import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPoint;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPolygon;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.codecs.text.json.JsonCodecUtils;
import com.datastax.oss.dsbulk.tests.assertions.TestAssertions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonNodeToPolygonCodecTest {

  private List<String> nullStrings = Lists.newArrayList("NULL");
  private Polygon polygon =
      new DefaultPolygon(
          new DefaultPoint(30, 10),
          new DefaultPoint(10, 20),
          new DefaultPoint(20, 40),
          new DefaultPoint(40, 40));
  private ObjectMapper objectMapper = JsonCodecUtils.getObjectMapper();
  private JsonNode geoJsonNode =
      objectMapper.readTree(
          "{\"type\":\"Polygon\",\"coordinates\":[[[30.0,10.0],[10.0,20.0],[20.0,40.0],[40.0,40.0],[30.0,10.0]]]}");

  JsonNodeToPolygonCodecTest() throws IOException {}

  @Test
  void should_convert_from_valid_external() throws IOException {
    JsonNodeToPolygonCodec codec = new JsonNodeToPolygonCodec(objectMapper, nullStrings);
    TestAssertions.assertThat(codec)
        .convertsFromExternal(
            JsonCodecUtils.JSON_NODE_FACTORY.textNode(
                "'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'"))
        .toInternal(polygon)
        .convertsFromExternal(
            JsonCodecUtils.JSON_NODE_FACTORY.textNode(
                " polygon ((30 10, 40 40, 20 40, 10 20, 30 10)) "))
        .toInternal(polygon)
        .convertsFromExternal(
            JsonCodecUtils.JSON_NODE_FACTORY.textNode(objectMapper.writeValueAsString(geoJsonNode)))
        .toInternal(polygon)
        .convertsFromExternal(geoJsonNode)
        .toInternal(polygon)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JsonCodecUtils.JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(JsonCodecUtils.JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    JsonNodeToPolygonCodec codec = new JsonNodeToPolygonCodec(objectMapper, nullStrings);
    TestAssertions.assertThat(codec).convertsFromInternal(polygon).toExternal(geoJsonNode);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    JsonNodeToPolygonCodec codec = new JsonNodeToPolygonCodec(objectMapper, nullStrings);
    TestAssertions.assertThat(codec)
        .cannotConvertFromExternal(
            JsonCodecUtils.JSON_NODE_FACTORY.textNode("not a valid polygon literal"));
  }
}
