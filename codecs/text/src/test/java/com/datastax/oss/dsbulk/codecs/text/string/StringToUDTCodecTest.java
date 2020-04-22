/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs.text.string;

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.dsbulk.codecs.ConversionContext;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToUDTCodecTest {

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

  private StringToUDTCodec udtCodec1;
  private StringToUDTCodec udtCodec2;
  private StringToUDTCodec udtCodec3;
  private StringToUDTCodec udtCodec4;

  @BeforeEach
  void setUp() {
    ConversionContext context1 = new TextConversionContext().setNullStrings("NULL", "");
    ConversionContext context2 = new TextConversionContext().setAllowExtraFields(true);
    ConversionContext context3 = new TextConversionContext().setAllowMissingFields(true);
    ConvertingCodecFactory codecFactory1 = new ConvertingCodecFactory(context1);
    ConvertingCodecFactory codecFactory2 = new ConvertingCodecFactory(context2);
    ConvertingCodecFactory codecFactory3 = new ConvertingCodecFactory(context3);
    udtCodec1 =
        (StringToUDTCodec)
            codecFactory1.<String, UdtValue>createConvertingCodec(udt1, GenericType.STRING, true);
    udtCodec2 =
        (StringToUDTCodec)
            codecFactory1.<String, UdtValue>createConvertingCodec(udt2, GenericType.STRING, true);
    udtCodec3 =
        (StringToUDTCodec)
            codecFactory2.<String, UdtValue>createConvertingCodec(udt3, GenericType.STRING, true);
    udtCodec4 =
        (StringToUDTCodec)
            codecFactory3.<String, UdtValue>createConvertingCodec(udt4, GenericType.STRING, true);
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(udtCodec1)
        .convertsFromExternal("{\"F1A\":42,\"f1b\":{\"foo\":1234.56,\"\":0.12}}")
        .toInternal(udt1Value)
        .convertsFromExternal("{'F1A':42,'f1b':{'foo':1234.56,'':0.12}}")
        .toInternal(udt1Value)
        .convertsFromExternal(
            "{ \"f1b\" :  { \"foo\" : \"1,234.56\" , \"\" : \"0000.12000\" } , \"F1A\" : \"42.00\" }")
        .toInternal(udt1Value)
        .convertsFromExternal("{ \"f1b\" :  { } , \"F1A\" :  null }")
        .toInternal(udt1ValueEmpty)
        .convertsFromExternal("{ \"f1b\" :  null , \"F1A\" :  null }")
        .toInternal(udt1ValueEmpty)
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("")
        .toInternal(null);
    assertThat(udtCodec2)
        .convertsFromExternal(
            "{\"f2a\":{\"F1A\":42,\"f1b\":{\"foo\":1234.56,\"\":0.12}},\"f2b\":[\"2017-09-22\"]}")
        .toInternal(udt2Value)
        .convertsFromExternal(
            "{'f2a':{'F1A':42,'f1b':{'foo':1234.56,'':0.12}},'f2b':['2017-09-22']}")
        .toInternal(udt2Value)
        .convertsFromExternal(
            "{ \"f2b\" :  [ \"2017-09-22\" ] , \"f2a\" : { \"f1b\" :  { \"foo\" : \"1,234.56\" , \"\" : \"0000.12000\" } , \"F1A\" : \"42.00\" } }")
        .toInternal(udt2Value)
        .convertsFromExternal("{ \"f2b\" :  null , \"f2a\" :  null }")
        .toInternal(udt2ValueEmpty)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("")
        .toInternal(null);
    // should allow extra fields
    assertThat(udtCodec3)
        .convertsFromExternal("{\"f1\":42,\"f2\":42}")
        .toInternal(udt3Value)
        .convertsFromExternal("{\"f1\":42,\"f2\":42,\"f3\":42}")
        .toInternal(udt3Value)
        .convertsFromExternal("[42,42]")
        .toInternal(udt3Value)
        .convertsFromExternal("[42,42,42]")
        .toInternal(udt3Value)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("")
        .toInternal(null);
    // should allow missing fields
    assertThat(udtCodec4)
        .convertsFromExternal("{\"f1\":42,\"f2\":42}")
        .toInternal(udt4Value)
        .convertsFromExternal("{\"f1\":42}")
        .toInternal(udt4ValuePartial)
        .convertsFromExternal("{}")
        .toInternal(udt4ValueEmpty)
        .convertsFromExternal("[42,42]")
        .toInternal(udt4Value)
        .convertsFromExternal("[42]")
        .toInternal(udt4ValuePartial)
        .convertsFromExternal("[]")
        .toInternal(udt4ValueEmpty)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(udtCodec1)
        .convertsFromInternal(udt1Value)
        .toExternal("{\"F1A\":42,\"f1b\":{\"foo\":1234.56,\"\":0.12}}")
        .convertsFromInternal(udt1.newValue())
        .toExternal("{\"F1A\":null,\"f1b\":{}}")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(udtCodec1)
        .cannotConvertFromExternal("{\"F1A\":42}")
        .cannotConvertFromExternal("[42]")
        .cannotConvertFromExternal("{\"not a valid input\":\"foo\"}");
    // should not allow missing fields
    assertThat(udtCodec3)
        .cannotConvertFromExternal("{\"f1\":42}")
        .cannotConvertFromExternal("[42]")
        .cannotConvertFromExternal("[]");
    // should not allow extra fields
    assertThat(udtCodec4)
        .cannotConvertFromExternal("{\"f1\":42,\"f2\":42,\"f3\":42}")
        .cannotConvertFromExternal("[42,42,42]");
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
