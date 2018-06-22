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
import static com.datastax.oss.driver.shaded.guava.common.collect.Lists.newArrayList;

import com.datastax.dsbulk.commons.codecs.json.JsonCodecUtils;
import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultLineString;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPoint;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonNodeToLineStringCodecTest {

  private List<String> nullStrings = newArrayList("NULL");
  private LineString lineString =
      new DefaultLineString(
          new DefaultPoint(30, 10), new DefaultPoint(10, 30), new DefaultPoint(40, 40));
  private ObjectMapper objectMapper = JsonCodecUtils.getObjectMapper();
  private JsonNode geoJsonNode =
      objectMapper.readTree(
          "{\"type\":\"LineString\",\"coordinates\":[[30.0,10.0],[10.0,30.0],[40.0,40.0]]}");

  JsonNodeToLineStringCodecTest() throws IOException {}

  @Test
  void should_convert_from_valid_external() throws IOException {
    JsonNodeToLineStringCodec codec = new JsonNodeToLineStringCodec(objectMapper, nullStrings);
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("'LINESTRING (30 10, 10 30, 40 40)'"))
        .toInternal(lineString)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(" linestring (30 10, 10 30, 40 40) "))
        .toInternal(lineString)
        .convertsFromExternal(
            JSON_NODE_FACTORY.textNode(objectMapper.writeValueAsString(geoJsonNode)))
        .toInternal(lineString)
        .convertsFromExternal(geoJsonNode)
        .toInternal(lineString)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    JsonNodeToLineStringCodec codec = new JsonNodeToLineStringCodec(objectMapper, nullStrings);
    assertThat(codec).convertsFromInternal(lineString).toExternal(geoJsonNode);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    JsonNodeToLineStringCodec codec = new JsonNodeToLineStringCodec(objectMapper, nullStrings);
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid linestring literal"));
  }
}
