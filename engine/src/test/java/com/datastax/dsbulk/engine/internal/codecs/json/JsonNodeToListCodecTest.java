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
import static com.google.common.collect.Lists.newArrayList;

import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonNodeToListCodecTest {

  private final ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  private JsonNodeToListCodec<Double> codec1;

  private JsonNodeToListCodec<String> codec2;

  @BeforeEach
  void setUp() {
    ExtendedCodecRegistry codecRegistry = newCodecRegistry("nullStrings = [NULL]");
    codec1 =
        (JsonNodeToListCodec<Double>)
            codecRegistry.codecFor(
                DataTypes.listOf(DataTypes.DOUBLE), GenericType.of(JsonNode.class));
    codec2 =
        (JsonNodeToListCodec<String>)
            codecRegistry.codecFor(
                DataTypes.listOf(DataTypes.TEXT), GenericType.of(JsonNode.class));
  }

  @Test
  void should_convert_from_valid_external() throws Exception {
    assertThat(codec1)
        .convertsFromExternal(objectMapper.readTree("[1,2,3]"))
        .toInternal(newArrayList(1d, 2d, 3d))
        .convertsFromExternal(objectMapper.readTree(" [  1 , 2 , 3 ] "))
        .toInternal(newArrayList(1d, 2d, 3d))
        .convertsFromExternal(objectMapper.readTree("[1234.56,78900]"))
        .toInternal(newArrayList(1234.56d, 78900d))
        .convertsFromExternal(objectMapper.readTree("[\"1,234.56\",\"78,900\"]"))
        .toInternal(newArrayList(1234.56d, 78900d))
        .convertsFromExternal(objectMapper.readTree("[,]"))
        .toInternal(newArrayList(null, null))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree("[]"))
        .toInternal(ImmutableList.of())
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
    assertThat(codec2)
        .convertsFromExternal(objectMapper.readTree("[\"foo\",\"bar\"]"))
        .toInternal(newArrayList("foo", "bar"))
        .convertsFromExternal(objectMapper.readTree("['foo','bar']"))
        .toInternal(newArrayList("foo", "bar"))
        .convertsFromExternal(objectMapper.readTree(" [ \"foo\" , \"bar\" ] "))
        .toInternal(newArrayList("foo", "bar"))
        .convertsFromExternal(objectMapper.readTree("[ \"\\\"foo\\\"\" , \"\\\"bar\\\"\" ]"))
        .toInternal(newArrayList("\"foo\"", "\"bar\""))
        .convertsFromExternal(
            objectMapper.readTree("[ \"\\\"fo\\\\o\\\"\" , \"\\\"ba\\\\r\\\"\" ]"))
        .toInternal(newArrayList("\"fo\\o\"", "\"ba\\r\""))
        .convertsFromExternal(objectMapper.readTree("[,]"))
        .toInternal(newArrayList(null, null))
        .convertsFromExternal(objectMapper.readTree("[null,null]"))
        .toInternal(newArrayList(null, null))
        // DAT-297: don't apply nullStrings to inner elements
        .convertsFromExternal(objectMapper.readTree("[\"\",\"\"]"))
        .toInternal(newArrayList("", ""))
        .convertsFromExternal(objectMapper.readTree("['','']"))
        .toInternal(newArrayList("", ""))
        .convertsFromExternal(objectMapper.readTree("[\"NULL\",\"NULL\"]"))
        .toInternal(newArrayList("NULL", "NULL"))
        .convertsFromExternal(objectMapper.readTree("['NULL','NULL']"))
        .toInternal(newArrayList("NULL", "NULL"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree("[]"))
        .toInternal(ImmutableList.of())
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() throws Exception {
    assertThat(codec1)
        .convertsFromInternal(newArrayList(1d, 2d, 3d))
        .toExternal(objectMapper.readTree("[1.0,2.0,3.0]"))
        .convertsFromInternal(newArrayList(1234.56d, 78900d))
        .toExternal(objectMapper.readTree("[1234.56,78900.0]"))
        .convertsFromInternal(newArrayList(1d, null))
        .toExternal(objectMapper.readTree("[1.0,null]"))
        .convertsFromInternal(newArrayList(null, 0d))
        .toExternal(objectMapper.readTree("[null,0.0]"))
        .convertsFromInternal(newArrayList(null, null))
        .toExternal(objectMapper.readTree("[null,null]"))
        .convertsFromInternal(ImmutableList.of())
        .toExternal(objectMapper.readTree("[]"))
        .convertsFromInternal(null)
        .toExternal(null);
    assertThat(codec2)
        .convertsFromInternal(newArrayList("foo", "bar"))
        .toExternal(objectMapper.readTree("[\"foo\",\"bar\"]"))
        .convertsFromInternal(newArrayList("\"foo\"", "\"bar\""))
        .toExternal(objectMapper.readTree("[\"\\\"foo\\\"\",\"\\\"bar\\\"\"]"))
        .convertsFromInternal(newArrayList("\\foo\\", "\\bar\\"))
        .toExternal(objectMapper.readTree("[\"\\\\foo\\\\\",\"\\\\bar\\\\\"]"))
        .convertsFromInternal(newArrayList(",foo,", ",bar,"))
        .toExternal(objectMapper.readTree("[\",foo,\",\",bar,\"]"))
        .convertsFromInternal(newArrayList("", ""))
        .toExternal(objectMapper.readTree("[\"\",\"\"]"))
        .convertsFromInternal(newArrayList(null, null))
        .toExternal(objectMapper.readTree("[null,null]"))
        .convertsFromInternal(ImmutableList.of())
        .toExternal(objectMapper.readTree("[]"))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() throws Exception {
    assertThat(codec1)
        .cannotConvertFromExternal(objectMapper.readTree("[1,\"not a valid double\"]"));
    assertThat(codec1)
        .cannotConvertFromExternal(objectMapper.readTree("{ \"not a valid array\" : 42 }"));
    assertThat(codec1).cannotConvertFromExternal(objectMapper.readTree("42"));
  }
}
