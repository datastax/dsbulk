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
package com.datastax.oss.dsbulk.codecs.text.json.dse;

import static com.datastax.oss.dsbulk.codecs.text.json.JsonCodecUtils.JSON_NODE_FACTORY;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPoint;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPolygon;
import com.datastax.oss.driver.api.core.data.ByteUtils;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.codecs.api.format.geo.JsonGeoFormat;
import com.datastax.oss.dsbulk.codecs.api.format.geo.WellKnownBinaryGeoFormat;
import com.datastax.oss.dsbulk.codecs.api.format.geo.WellKnownTextGeoFormat;
import com.datastax.oss.dsbulk.codecs.text.json.JsonCodecUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonNodeToPolygonCodecTest {

  private final List<String> nullStrings = Lists.newArrayList("NULL");
  private final Polygon polygon =
      new DefaultPolygon(
          new DefaultPoint(30, 10),
          new DefaultPoint(10, 20),
          new DefaultPoint(20, 40),
          new DefaultPoint(40, 40));
  private final ObjectMapper objectMapper = JsonCodecUtils.getObjectMapper();
  private final JsonNode geoJsonNode =
      objectMapper.readTree(
          "{\"type\":\"Polygon\",\"coordinates\":[[[30.0,10.0],[10.0,20.0],[20.0,40.0],[40.0,40.0],[30.0,10.0]]]}");
  private final JsonNode wktJsonNode =
      objectMapper.getNodeFactory().textNode("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))");
  private final JsonNode wkbJsonNode =
      objectMapper.getNodeFactory().binaryNode(ByteUtils.getArray(polygon.asWellKnownBinary()));
  private final JsonNode wkbBase64JsonNode =
      objectMapper
          .getNodeFactory()
          .textNode(
              "AQMAAAABAAAABQAAAAAAAAAAAD5AAAAAAAAAJEAAAAAAAABEQAAAAAAAAERAAAAAAAAANEAAAAAAAABEQAAAAAAAACRAAAAAAAAANEAAAAAAAAA+QAAAAAAAACRA");
  private final JsonNode wkbHexJsonNode =
      objectMapper
          .getNodeFactory()
          .textNode(
              "0x010300000001000000050000000000000000003e4000000000000024400000000000004440000000000000444"
                  + "000000000000034400000000000004440000000000000244000000000000034400000000000003e400000000000002440");

  JsonNodeToPolygonCodecTest() throws IOException {}

  @Test
  void should_convert_from_valid_external() throws IOException {
    JsonNodeToPolygonCodec codec =
        new JsonNodeToPolygonCodec(objectMapper, WellKnownTextGeoFormat.INSTANCE, nullStrings);
    assertThat(codec)
        .convertsFromExternal(
            JSON_NODE_FACTORY.textNode("'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'"))
        .toInternal(polygon)
        .convertsFromExternal(
            JSON_NODE_FACTORY.textNode(" polygon ((30 10, 40 40, 20 40, 10 20, 30 10)) "))
        .toInternal(polygon)
        .convertsFromExternal(wkbJsonNode)
        .toInternal(polygon)
        .convertsFromExternal(
            JSON_NODE_FACTORY.textNode(
                "AQMAAAABAAAABQAAAAAAAAAAAD5AAAAAAAAAJEAAAAAAAABEQAAAAAAAAERAAAAAAAAANEAAAAAAAABEQAAAAAAAACRAAAAAAAAANEAAAAAAAAA+QAAAAAAAACRA"))
        .toInternal(polygon)
        .convertsFromExternal(
            JSON_NODE_FACTORY.textNode(
                "0x010300000001000000050000000000000000003e4000000000000024400000000000004440000000000000444"
                    + "000000000000034400000000000004440000000000000244000000000000034400000000000003e400000000000002440"))
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
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    JsonNodeToPolygonCodec codec =
        new JsonNodeToPolygonCodec(objectMapper, WellKnownTextGeoFormat.INSTANCE, nullStrings);
    assertThat(codec).convertsFromInternal(polygon).toExternal(wktJsonNode);
    codec = new JsonNodeToPolygonCodec(objectMapper, JsonGeoFormat.INSTANCE, nullStrings);
    assertThat(codec).convertsFromInternal(polygon).toExternal(geoJsonNode);
    codec =
        new JsonNodeToPolygonCodec(
            objectMapper, WellKnownBinaryGeoFormat.BASE64_INSTANCE, nullStrings);
    assertThat(codec).convertsFromInternal(polygon).toExternal(wkbBase64JsonNode);
    codec =
        new JsonNodeToPolygonCodec(
            objectMapper, WellKnownBinaryGeoFormat.HEX_INSTANCE, nullStrings);
    assertThat(codec).convertsFromInternal(polygon).toExternal(wkbHexJsonNode);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    JsonNodeToPolygonCodec codec =
        new JsonNodeToPolygonCodec(objectMapper, WellKnownTextGeoFormat.INSTANCE, nullStrings);
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid polygon literal"));
  }
}
