/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json;

import static com.datastax.dsbulk.commons.codecs.json.JsonCodecUtils.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistryBuilder;
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

  // user types

  private final UserDefinedType udt1 =
      new UserDefinedTypeBuilder("ks", "udt")
          .withField("\"F1A\"", DataTypes.INT)
          .withField("f1b", DataTypes.mapOf(DataTypes.TEXT, DataTypes.DOUBLE))
          .build();

  private final UserDefinedType udt2 =
      new UserDefinedTypeBuilder("ks", "udt")
          .withField("f2a", udt1)
          .withField("f2b", DataTypes.listOf(DataTypes.DATE))
          .build();

  private final UserDefinedType udt3 =
      new UserDefinedTypeBuilder("ks", "udt")
          .withField("f1", DataTypes.INT)
          .withField("f2", DataTypes.INT)
          .build();

  private final UserDefinedType udt4 =
      new UserDefinedTypeBuilder("ks", "udt")
          .withField("f1", DataTypes.INT)
          .withField("f2", DataTypes.INT)
          .build();

  // user type values

  private final UdtValue udt1Value =
      udt1.newValue()
          .setInt("\"F1A\"", 42)
          .setMap("f1b", newMap("foo", 1234.56d, "", 0.12d), String.class, Double.class);
  private final UdtValue udt1ValueEmpty = udt1.newValue().setToNull("\"F1A\"").setToNull("f1b");

  private final UdtValue udt2Value =
      udt2.newValue()
          .setUdtValue("f2a", udt1Value)
          .set("f2b", newList(LocalDate.of(2017, 9, 22)), TypeCodecs.listOf(TypeCodecs.DATE));
  private final UdtValue udt2ValueEmpty = udt2.newValue().setToNull("f2a").setToNull("f2b");

  private final UdtValue udt3Value = udt3.newValue().setInt("f1", 42).setInt("f2", 42);

  private final UdtValue udt4Value = udt4.newValue().setInt("f1", 42).setInt("f2", 42);
  private final UdtValue udt4ValuePartial = udt4.newValue().setInt("f1", 42);
  private final UdtValue udt4ValueEmpty = udt4.newValue();

  // codecs

  private JsonNodeToUDTCodec udtCodec1;
  private JsonNodeToUDTCodec udtCodec2;
  private JsonNodeToUDTCodec udtCodec3;
  private JsonNodeToUDTCodec udtCodec4;

  @BeforeEach
  void setUp() {
    ExtendedCodecRegistry codecRegistry1 =
        new ExtendedCodecRegistryBuilder().withNullStrings("NULL", "").build();
    udtCodec1 = (JsonNodeToUDTCodec) codecRegistry1.codecFor(udt1, GenericType.of(JsonNode.class));
    udtCodec2 = (JsonNodeToUDTCodec) codecRegistry1.codecFor(udt2, GenericType.of(JsonNode.class));
    ExtendedCodecRegistry codecRegistry2 =
        new ExtendedCodecRegistryBuilder().allowExtraFields(true).build();
    udtCodec3 = (JsonNodeToUDTCodec) codecRegistry2.codecFor(udt3, GenericType.of(JsonNode.class));
    ExtendedCodecRegistry codecRegistry3 =
        new ExtendedCodecRegistryBuilder().allowMissingFields(true).build();
    udtCodec4 = (JsonNodeToUDTCodec) codecRegistry3.codecFor(udt4, GenericType.of(JsonNode.class));
  }

  @Test
  void should_convert_from_valid_external() throws Exception {
    assertThat(udtCodec1)
        .convertsFromExternal(
            objectMapper.readTree("{\"F1A\":42,\"f1b\":{\"foo\":1234.56,\"\":0.12}}"))
        .toInternal(udt1Value)
        .convertsFromExternal(objectMapper.readTree("{'F1A':42,'f1b':{'foo':1234.56,'':0.12}}"))
        .toInternal(udt1Value)
        .convertsFromExternal(
            objectMapper.readTree(
                "{ \"f1b\" :  { \"foo\" : \"1,234.56\" , \"\" : \"0000.12000\" } , \"F1A\" : \"42.00\" }"))
        .toInternal(udt1Value)
        .convertsFromExternal(objectMapper.readTree("[42,{\"foo\":1234.56,\"\":0.12}]"))
        .toInternal(udt1Value)
        .convertsFromExternal(objectMapper.readTree("{ \"f1b\" :  { } , \"F1A\" :  null }"))
        .toInternal(udt1ValueEmpty)
        .convertsFromExternal(objectMapper.readTree("{ \"f1b\" :  null , \"F1A\" :  null }"))
        .toInternal(udt1ValueEmpty)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
    assertThat(udtCodec2)
        .convertsFromExternal(
            objectMapper.readTree(
                "{\"f2a\":{\"F1A\":42,\"f1b\":{\"foo\":1234.56,\"\":0.12}},\"f2b\":[\"2017-09-22\"]}"))
        .toInternal(udt2Value)
        .convertsFromExternal(
            objectMapper.readTree(
                "{'f2a':{'F1A':42,'f1b':{'foo':1234.56,'':0.12}},'f2b':['2017-09-22']}"))
        .toInternal(udt2Value)
        .convertsFromExternal(
            objectMapper.readTree(
                "{ \"f2b\" :  [ \"2017-09-22\" ] , \"f2a\" : { \"f1b\" :  { \"foo\" : \"1,234.56\" , \"\" : \"0000.12000\" } , \"F1A\" : \"42.00\" } }"))
        .toInternal(udt2Value)
        .convertsFromExternal(objectMapper.readTree("{ \"f2b\" :  null , \"f2a\" :  null }"))
        .toInternal(udt2ValueEmpty)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
    // should allow extra fields
    assertThat(udtCodec3)
        .convertsFromExternal(objectMapper.readTree("{\"f1\":42,\"f2\":42}"))
        .toInternal(udt3Value)
        .convertsFromExternal(objectMapper.readTree("{\"f1\":42,\"f2\":42,\"f3\":42}"))
        .toInternal(udt3Value)
        .convertsFromExternal(objectMapper.readTree("[42,42]"))
        .toInternal(udt3Value)
        .convertsFromExternal(objectMapper.readTree("[42,42,42]"))
        .toInternal(udt3Value)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
    // should allow missing fields
    assertThat(udtCodec4)
        .convertsFromExternal(objectMapper.readTree("{\"f1\":42,\"f2\":42}"))
        .toInternal(udt4Value)
        .convertsFromExternal(objectMapper.readTree("{\"f1\":42}"))
        .toInternal(udt4ValuePartial)
        .convertsFromExternal(objectMapper.readTree("{}"))
        .toInternal(udt4ValueEmpty)
        .convertsFromExternal(objectMapper.readTree("[42,42]"))
        .toInternal(udt4Value)
        .convertsFromExternal(objectMapper.readTree("[42]"))
        .toInternal(udt4ValuePartial)
        .convertsFromExternal(objectMapper.readTree("[]"))
        .toInternal(udt4ValueEmpty)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree(""))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() throws Exception {
    assertThat(udtCodec1)
        .convertsFromInternal(udt1Value)
        .toExternal(objectMapper.readTree("{\"F1A\":42,\"f1b\":{\"foo\":1234.56,\"\":0.12}}"))
        .convertsFromInternal(udt1.newValue())
        .toExternal(objectMapper.readTree("{\"F1A\":null,\"f1b\":{}}"))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() throws Exception {
    assertThat(udtCodec1)
        .cannotConvertFromExternal(objectMapper.readTree("42"))
        .cannotConvertFromExternal(objectMapper.readTree("{\"F1A\":42}"))
        .cannotConvertFromExternal(objectMapper.readTree("[42]"))
        .cannotConvertFromExternal(objectMapper.readTree("{\"F1A\":null,\"f1c\":{}}"))
        .cannotConvertFromExternal(objectMapper.readTree("{\"not a valid input\":\"foo\"}"));
    // should not allow missing fields
    assertThat(udtCodec3)
        .cannotConvertFromExternal(objectMapper.readTree("{\"f1\":42}"))
        .cannotConvertFromExternal(objectMapper.readTree("[42]"))
        .cannotConvertFromExternal(objectMapper.readTree("[]"));
    // should not allow extra fields
    assertThat(udtCodec4)
        .cannotConvertFromExternal(objectMapper.readTree("{\"f1\":42,\"f2\":42,\"f3\":42}"))
        .cannotConvertFromExternal(objectMapper.readTree("[42,42,42]"));
    // tests for error messages
    assertThatThrownBy(
            () ->
                udtCodec1.externalToInternal(objectMapper.readTree("{\"F1A\":null,\"f1c\":null}")))
        .isInstanceOf(JsonSchemaMismatchException.class)
        .hasMessageContaining("1 extraneous field: 'f1c'")
        .hasMessageContaining("1 missing field: 'f1b'");
    assertThatThrownBy(
            () -> udtCodec3.externalToInternal(objectMapper.readTree("{\"f1\":null,\"f3\":null}")))
        .isInstanceOf(JsonSchemaMismatchException.class)
        .satisfies(
            t ->
                assertThat(t.getMessage())
                    .contains("1 missing field: 'f2'")
                    .doesNotContain("1 extraneous field: 'f3'"));
    assertThatThrownBy(
            () -> udtCodec4.externalToInternal(objectMapper.readTree("{\"f1\":null,\"f3\":null}")))
        .isInstanceOf(JsonSchemaMismatchException.class)
        .satisfies(
            t ->
                assertThat(t.getMessage())
                    .contains("1 extraneous field: 'f3'")
                    .doesNotContain("1 missing field: 'f2'"));
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
