/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.codecs.CodecTestUtils.newCodecRegistry;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;

import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonNodeToMapCodecTest {

  private final ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  private JsonNodeToMapCodec<Double, List<String>> codec;

  @BeforeEach
  void setUp() {
    codec =
        (JsonNodeToMapCodec<Double, List<String>>)
            newCodecRegistry(
                    "nullStrings = [NULL], roundingStrategy = HALF_EVEN, formatNumbers = true")
                .codecFor(
                    DataTypes.mapOf(DataTypes.DOUBLE, DataTypes.listOf(DataTypes.TEXT)),
                    GenericType.of(JsonNode.class));
  }

  @Test
  void should_convert_from_valid_external() throws Exception {
    assertThat(codec)
        .convertsFromExternal(objectMapper.readTree("{1 : [\"foo\", \"bar\"], 2:[\"qix\"]}"))
        .toInternal(map(1d, list("foo", "bar"), 2d, list("qix")))
        .convertsFromExternal(
            objectMapper.readTree("{ '1234.56' : ['foo', 'bar'], '0.12' : ['qix'] }"))
        .toInternal(map(1234.56d, list("foo", "bar"), 0.12d, list("qix")))
        .convertsFromExternal(objectMapper.readTree("{ '1,234.56' : ['foo'] , '.12' : ['bar']}"))
        .toInternal(map(1234.56d, list("foo"), 0.12d, list("bar")))
        .convertsFromExternal(objectMapper.readTree("{1: [], 2 :[]}"))
        .toInternal(map(1d, list(), 2d, list()))
        // DAT-297: don't apply nullStrings to inner elements
        .convertsFromExternal(objectMapper.readTree("{1: [\"NULL\"], 2: ['NULL']}"))
        .toInternal(map(1d, list("NULL"), 2d, list("NULL")))
        .convertsFromExternal(objectMapper.readTree("{1: [\"\"], 2: ['']}"))
        .toInternal(map(1d, list(""), 2d, list("")))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree("{}"))
        .toInternal(ImmutableMap.of())
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() throws Exception {
    assertThat(codec)
        .convertsFromInternal(map(1d, list("foo", "bar"), 2d, list("qix")))
        .toExternal(objectMapper.readTree("{\"1\":[\"foo\",\"bar\"],\"2\":[\"qix\"]}"))
        .convertsFromInternal(map(1234.56d, list("foo", "bar"), 0.12d, list("qix")))
        .toExternal(objectMapper.readTree("{\"1,234.56\":[\"foo\",\"bar\"],\"0.12\":[\"qix\"]}"))
        .convertsFromInternal(map(1d, list(""), 2d, list("")))
        .toExternal(objectMapper.readTree("{\"1\":[\"\"],\"2\":[\"\"]}"))
        .convertsFromInternal(map(1d, null, 2d, list()))
        .toExternal(objectMapper.readTree("{\"1\":null,\"2\":[]}"))
        .convertsFromInternal(ImmutableMap.of())
        .toExternal(objectMapper.readTree("{}"))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() throws Exception {
    assertThat(codec)
        .cannotConvertFromExternal(objectMapper.readTree("{\"not a valid input\":\"foo\"}"))
        .cannotConvertFromExternal(objectMapper.readTree("[1,\"not a valid object\"]"))
        .cannotConvertFromExternal(objectMapper.readTree("42"));
  }

  private static Map<Double, List<String>> map(
      Double k1, List<String> v1, Double k2, List<String> v2) {
    Map<Double, List<String>> map = new LinkedHashMap<>();
    map.put(k1, v1);
    map.put(k2, v2);
    return map;
  }

  private static List<String> list(String... elements) {
    return Arrays.asList(elements);
  }
}
