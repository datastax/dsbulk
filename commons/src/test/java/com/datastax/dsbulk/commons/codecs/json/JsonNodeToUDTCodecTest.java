/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json;

import static com.datastax.dsbulk.commons.codecs.CodecTestUtils.newCodecRegistry;
import static com.datastax.dsbulk.commons.codecs.json.JsonCodecUtils.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;

import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistry;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonNodeToUDTCodecTest {

  private final ObjectMapper objectMapper = JsonCodecUtils.getObjectMapper();

  private final UserDefinedType udt1 =
      new UserDefinedTypeBuilder("ks", "udt")
          .withField("f1a", DataTypes.INT)
          .withField("f1b", DataTypes.mapOf(DataTypes.TEXT, DataTypes.DOUBLE))
          .build();

  private final UserDefinedType udt2 =
      new UserDefinedTypeBuilder("ks", "udt")
          .withField("f2a", udt1)
          .withField("f2b", DataTypes.listOf(DataTypes.DATE))
          .build();

  private final UdtValue udt1Empty = udt1.newValue().setToNull("f1a").setToNull("f1b");

  private final UdtValue udt2Empty = udt2.newValue().setToNull("f2a").setToNull("f2b");

  private final UdtValue udt1Value =
      udt1.newValue()
          .setInt("f1a", 42)
          .setMap("f1b", newMap("foo", 1234.56d, "", 0.12d), String.class, Double.class);

  private final UdtValue udt2Value =
      udt2.newValue()
          .setUdtValue("f2a", udt1Value)
          .set("f2b", newList(LocalDate.of(2017, 9, 22)), TypeCodecs.listOf(TypeCodecs.DATE));

  private JsonNodeToUDTCodec udtCodec1;

  private JsonNodeToUDTCodec udtCodec2;

  @BeforeEach
  void setUp() {
    ExtendedCodecRegistry codecRegistry = newCodecRegistry("nullStrings = [NULL, \"\"]");
    udtCodec1 = (JsonNodeToUDTCodec) codecRegistry.codecFor(udt1, GenericType.of(JsonNode.class));
    udtCodec2 = (JsonNodeToUDTCodec) codecRegistry.codecFor(udt2, GenericType.of(JsonNode.class));
  }

  @Test
  void should_convert_from_valid_external() throws Exception {
    assertThat(udtCodec1)
        .convertsFromExternal(
            objectMapper.readTree("{\"f1a\":42,\"f1b\":{\"foo\":1234.56,\"\":0.12}}"))
        .toInternal(udt1Value)
        .convertsFromExternal(objectMapper.readTree("{'f1a':42,'f1b':{'foo':1234.56,'':0.12}}"))
        .toInternal(udt1Value)
        .convertsFromExternal(
            objectMapper.readTree(
                "{ \"f1b\" :  { \"foo\" : \"1,234.56\" , \"\" : \"0000.12000\" } , \"f1a\" : \"42.00\" }"))
        .toInternal(udt1Value)
        .convertsFromExternal(objectMapper.readTree("{ \"f1b\" :  { } , \"f1a\" :  null }"))
        .toInternal(udt1Empty)
        .convertsFromExternal(objectMapper.readTree("{ \"f1b\" :  null , \"f1a\" :  null }"))
        .toInternal(udt1Empty)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree("{}"))
        .toInternal(udt1Empty)
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
    assertThat(udtCodec2)
        .convertsFromExternal(
            objectMapper.readTree(
                "{\"f2a\":{\"f1a\":42,\"f1b\":{\"foo\":1234.56,\"\":0.12}},\"f2b\":[\"2017-09-22\"]}"))
        .toInternal(udt2Value)
        .convertsFromExternal(
            objectMapper.readTree(
                "{'f2a':{'f1a':42,'f1b':{'foo':1234.56,'':0.12}},'f2b':['2017-09-22']}"))
        .toInternal(udt2Value)
        .convertsFromExternal(
            objectMapper.readTree(
                "{ \"f2b\" :  [ \"2017-09-22\" ] , \"f2a\" : { \"f1b\" :  { \"foo\" : \"1,234.56\" , \"\" : \"0000.12000\" } , \"f1a\" : \"42.00\" } }"))
        .toInternal(udt2Value)
        .convertsFromExternal(objectMapper.readTree("{ \"f2b\" :  null , \"f2a\" :  null }"))
        .toInternal(udt2Empty)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree("{}"))
        .toInternal(udt2Empty)
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() throws Exception {
    assertThat(udtCodec1)
        .convertsFromInternal(udt1Value)
        .toExternal(objectMapper.readTree("{\"f1a\":42,\"f1b\":{\"foo\":1234.56,\"\":0.12}}"))
        .convertsFromInternal(udt1.newValue())
        .toExternal(objectMapper.readTree("{\"f1a\":null,\"f1b\":{}}"))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() throws Exception {
    assertThat(udtCodec1)
        .cannotConvertFromExternal(objectMapper.readTree("{\"f1a\":42}"))
        .cannotConvertFromExternal(objectMapper.readTree("[42]"))
        .cannotConvertFromExternal(objectMapper.readTree("{\"not a valid input\":\"foo\"}"));
  }

  @SuppressWarnings("SameParameterValue")
  private static Map<String, Double> newMap(String k1, Double v1, String k2, Double v2) {
    Map<String, Double> map = new LinkedHashMap<>();
    map.put(k1, v1);
    map.put(k2, v2);
    return map;
  }

  private static List<LocalDate> newList(LocalDate... elements) {
    return Arrays.asList(elements);
  }
}
