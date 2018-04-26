/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json.dse;

import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;

import com.datastax.driver.dse.geometry.Point;
import com.datastax.driver.dse.geometry.Polygon;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonNodeToPolygonCodecTest {

  private List<String> nullStrings = newArrayList("NULL");
  private Polygon polygon =
      new Polygon(new Point(30, 10), new Point(10, 20), new Point(20, 40), new Point(40, 40));
  private ObjectMapper objectMapper = CodecSettings.getObjectMapper();
  private JsonNode geoJsonNode =
      objectMapper.readTree(
          "{\"type\":\"Polygon\",\"coordinates\":[[[30.0,10.0],[10.0,20.0],[20.0,40.0],[40.0,40.0],[30.0,10.0]]]}");

  JsonNodeToPolygonCodecTest() throws IOException {}

  @Test
  void should_convert_from_valid_external() throws IOException {
    JsonNodeToPolygonCodec codec = new JsonNodeToPolygonCodec(objectMapper, nullStrings);
    assertThat(codec)
        .convertsFromExternal(
            JSON_NODE_FACTORY.textNode("'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'"))
        .toInternal(polygon)
        .convertsFromExternal(
            JSON_NODE_FACTORY.textNode(" polygon ((30 10, 40 40, 20 40, 10 20, 30 10)) "))
        .toInternal(polygon)
        .convertsFromExternal(
            JSON_NODE_FACTORY.textNode(objectMapper.writeValueAsString(geoJsonNode)))
        .toInternal(polygon)
        .convertsFromExternal(geoJsonNode)
        .toInternal(polygon)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.nullNode())
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    JsonNodeToPolygonCodec codec = new JsonNodeToPolygonCodec(objectMapper, nullStrings);
    assertThat(codec).convertsFromInternal(polygon).toExternal(geoJsonNode);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    JsonNodeToPolygonCodec codec = new JsonNodeToPolygonCodec(objectMapper, nullStrings);
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode(""))
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid polygon literal"));
  }
}
