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

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultLineString;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPoint;
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

class JsonNodeToLineStringCodecTest {

  private final List<String> nullStrings = Lists.newArrayList("NULL");
  private final LineString lineString =
      new DefaultLineString(
          new DefaultPoint(30, 10), new DefaultPoint(10, 30), new DefaultPoint(40, 40));
  private final ObjectMapper objectMapper = JsonCodecUtils.getObjectMapper();
  private final JsonNode geoJsonNode =
      objectMapper.readTree(
          "{\"type\":\"LineString\",\"coordinates\":[[30.0,10.0],[10.0,30.0],[40.0,40.0]]}");
  private final JsonNode wktJsonNode =
      objectMapper.getNodeFactory().textNode("LINESTRING (30 10, 10 30, 40 40)");
  private final JsonNode wkbBase64JsonNode =
      objectMapper
          .getNodeFactory()
          .textNode("AQIAAAADAAAAAAAAAAAAPkAAAAAAAAAkQAAAAAAAACRAAAAAAAAAPkAAAAAAAABEQAAAAAAAAERA");
  private final JsonNode wkbHexJsonNode =
      objectMapper
          .getNodeFactory()
          .textNode(
              "0x0102000000030000000000000000003e40000000000000244000000000000024400000000000003e4000000000000044400000000000004440");

  JsonNodeToLineStringCodecTest() throws IOException {}

  @Test
  void should_convert_from_valid_external() throws IOException {
    JsonNodeToLineStringCodec codec =
        new JsonNodeToLineStringCodec(objectMapper, WellKnownTextGeoFormat.INSTANCE, nullStrings);
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
    JsonNodeToLineStringCodec codec =
        new JsonNodeToLineStringCodec(objectMapper, WellKnownTextGeoFormat.INSTANCE, nullStrings);
    assertThat(codec).convertsFromInternal(lineString).toExternal(wktJsonNode);
    codec = new JsonNodeToLineStringCodec(objectMapper, JsonGeoFormat.INSTANCE, nullStrings);
    assertThat(codec).convertsFromInternal(lineString).toExternal(geoJsonNode);
    codec =
        new JsonNodeToLineStringCodec(
            objectMapper, WellKnownBinaryGeoFormat.BASE64_INSTANCE, nullStrings);
    assertThat(codec).convertsFromInternal(lineString).toExternal(wkbBase64JsonNode);
    codec =
        new JsonNodeToLineStringCodec(
            objectMapper, WellKnownBinaryGeoFormat.HEX_INSTANCE, nullStrings);
    assertThat(codec).convertsFromInternal(lineString).toExternal(wkbHexJsonNode);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    JsonNodeToLineStringCodec codec =
        new JsonNodeToLineStringCodec(objectMapper, WellKnownTextGeoFormat.INSTANCE, nullStrings);
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid linestring literal"));
  }
}
