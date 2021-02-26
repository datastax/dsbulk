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

import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPoint;
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

class JsonNodeToPointCodecTest {

  private final List<String> nullStrings = Lists.newArrayList("NULL");
  private final Point point = new DefaultPoint(-1.1, -2.2);
  private final ObjectMapper objectMapper = JsonCodecUtils.getObjectMapper();
  private final JsonNode geoJsonNode =
      objectMapper.readTree("{\"type\":\"Point\",\"coordinates\":[-1.1,-2.2]}");
  private final JsonNode wktJsonNode = objectMapper.getNodeFactory().textNode("POINT (-1.1 -2.2)");
  private final JsonNode wkbJsonNode =
      objectMapper.getNodeFactory().binaryNode(ByteUtils.getArray(point.asWellKnownBinary()));
  private final JsonNode wkbBase64JsonNode =
      objectMapper.getNodeFactory().textNode("AQEAAACamZmZmZnxv5qZmZmZmQHA");
  private final JsonNode wkbHexJsonNode =
      objectMapper.getNodeFactory().textNode("0x01010000009a9999999999f1bf9a999999999901c0");

  JsonNodeToPointCodecTest() throws IOException {}

  @Test
  void should_convert_from_valid_external() throws IOException {
    JsonNodeToPointCodec codec =
        new JsonNodeToPointCodec(objectMapper, WellKnownTextGeoFormat.INSTANCE, nullStrings);
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("'POINT (-1.1 -2.2)'"))
        .toInternal(point)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(" point (-1.1 -2.2) "))
        .toInternal(point)
        .convertsFromExternal(wkbJsonNode)
        .toInternal(point)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("AQEAAACamZmZmZnxv5qZmZmZmQHA"))
        .toInternal(point)
        .convertsFromExternal(
            JSON_NODE_FACTORY.textNode("0x01010000009a9999999999f1bf9a999999999901c0"))
        .toInternal(point)
        .convertsFromExternal(
            JSON_NODE_FACTORY.textNode(objectMapper.writeValueAsString(geoJsonNode)))
        .toInternal(point)
        .convertsFromExternal(geoJsonNode)
        .toInternal(point)
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
    JsonNodeToPointCodec codec =
        new JsonNodeToPointCodec(objectMapper, WellKnownTextGeoFormat.INSTANCE, nullStrings);
    assertThat(codec).convertsFromInternal(point).toExternal(wktJsonNode);
    codec = new JsonNodeToPointCodec(objectMapper, JsonGeoFormat.INSTANCE, nullStrings);
    assertThat(codec).convertsFromInternal(point).toExternal(geoJsonNode);
    codec =
        new JsonNodeToPointCodec(
            objectMapper, WellKnownBinaryGeoFormat.BASE64_INSTANCE, nullStrings);
    assertThat(codec).convertsFromInternal(point).toExternal(wkbBase64JsonNode);
    codec =
        new JsonNodeToPointCodec(objectMapper, WellKnownBinaryGeoFormat.HEX_INSTANCE, nullStrings);
    assertThat(codec).convertsFromInternal(point).toExternal(wkbHexJsonNode);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    JsonNodeToPointCodec codec =
        new JsonNodeToPointCodec(objectMapper, WellKnownTextGeoFormat.INSTANCE, nullStrings);
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid point literal"));
  }
}
