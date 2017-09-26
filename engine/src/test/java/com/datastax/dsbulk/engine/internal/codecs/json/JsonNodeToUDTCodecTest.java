/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.driver.core.DataType.cdouble;
import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.date;
import static com.datastax.driver.core.DataType.list;
import static com.datastax.driver.core.DataType.map;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.DriverCoreTestHooks.newField;
import static com.datastax.driver.core.DriverCoreTestHooks.newUserType;
import static com.datastax.driver.core.TypeCodec.userType;
import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToStringCodec;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.Test;

public class JsonNodeToUDTCodecTest {

  private ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  CodecRegistry codecRegistry = new CodecRegistry().register(LocalDateCodec.instance);

  private ThreadLocal<DecimalFormat> formatter =
      ThreadLocal.withInitial(
          () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US)));

  // UDT 1

  private UserType udt1 =
      newUserType(
          codecRegistry, newField("f1a", cint()), newField("f1b", map(varchar(), cdouble())));

  private final UDTValue udt1Empty = udt1.newValue().setToNull("f1a").setToNull("f1b");
  private final UDTValue udt1Value =
      udt1.newValue().setInt("f1a", 42).setMap("f1b", newMap("foo", 1234.56d, "bar", 0.12d));

  private ConvertingCodec f1aCodec = new JsonNodeToIntegerCodec(formatter);
  private ConvertingCodec f1bCodec =
      new JsonNodeToMapCodec<>(
          TypeCodec.map(TypeCodec.varchar(), TypeCodec.cdouble()),
          new StringToStringCodec(TypeCodec.varchar()),
          new JsonNodeToDoubleCodec(formatter),
          objectMapper);

  @SuppressWarnings("unchecked")
  private JsonNodeToUDTCodec udtCodec1 =
      new JsonNodeToUDTCodec(
          userType(udt1), ImmutableMap.of("f1a", f1aCodec, "f1b", f1bCodec), objectMapper);

  // UDT 2

  private UserType udt2 =
      newUserType(codecRegistry, newField("f2a", udt1), newField("f2b", list(date())));

  private final UDTValue udt2Empty = udt2.newValue().setToNull("f2a").setToNull("f2b");
  private final UDTValue udt2Value =
      udt2.newValue()
          .setUDTValue("f2a", udt1Value)
          .set("f2b", newList(LocalDate.of(2017, 9, 22)), TypeCodec.list(LocalDateCodec.instance));

  @SuppressWarnings("unchecked")
  private ConvertingCodec f2aCodec =
      new JsonNodeToUDTCodec(
          userType(udt1), ImmutableMap.of("f1a", f1aCodec, "f1b", f1bCodec), objectMapper);

  private ConvertingCodec f2bCodec =
      new JsonNodeToListCodec<>(
          TypeCodec.list(LocalDateCodec.instance),
          new JsonNodeToLocalDateCodec(CQL_DATE_TIME_FORMAT),
          objectMapper);

  @SuppressWarnings("unchecked")
  private JsonNodeToUDTCodec udtCodec2 =
      new JsonNodeToUDTCodec(
          userType(udt2), ImmutableMap.of("f2a", f2aCodec, "f2b", f2bCodec), objectMapper);

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(udtCodec1)
        .convertsFrom(objectMapper.readTree("{\"f1a\":42,\"f1b\":{\"foo\":1234.56,\"bar\":0.12}}"))
        .to(udt1Value)
        .convertsFrom(objectMapper.readTree("{'f1a':42,'f1b':{'foo':1234.56,'bar':0.12}}"))
        .to(udt1Value)
        .convertsFrom(
            objectMapper.readTree(
                "{ \"f1b\" :  { \"foo\" : \"1,234.56\" , \"bar\" : \"0000.12000\" } , \"f1a\" : \"42.00\" }"))
        .to(udt1Value)
        .convertsFrom(objectMapper.readTree("{ \"f1b\" :  { } , \"f1a\" :  ''}"))
        .to(udt1Empty)
        .convertsFrom(objectMapper.readTree("{ \"f1b\" :  null , \"f1a\" :  null }"))
        .to(udt1Empty)
        .convertsFrom(null)
        .to(null)
        .convertsFrom(objectMapper.readTree("{}"))
        .to(null)
        .convertsFrom(objectMapper.readTree(""))
        .to(null);
    assertThat(udtCodec2)
        .convertsFrom(
            objectMapper.readTree(
                "{\"f2a\":{\"f1a\":42,\"f1b\":{\"foo\":1234.56,\"bar\":0.12}},\"f2b\":[\"2017-09-22\"]}"))
        .to(udt2Value)
        .convertsFrom(
            objectMapper.readTree(
                "{'f2a':{'f1a':42,'f1b':{'foo':1234.56,'bar':0.12}},'f2b':['2017-09-22']}"))
        .to(udt2Value)
        .convertsFrom(
            objectMapper.readTree(
                "{ \"f2b\" :  [ \"2017-09-22\" ] , \"f2a\" : { \"f1b\" :  { \"foo\" : \"1,234.56\" , \"bar\" : \"0000.12000\" } , \"f1a\" : \"42.00\" } }"))
        .to(udt2Value)
        .convertsFrom(objectMapper.readTree("{ \"f2b\" :  null , \"f2a\" :  null }"))
        .to(udt2Empty)
        .convertsFrom(null)
        .to(null)
        .convertsFrom(objectMapper.readTree("{}"))
        .to(null)
        .convertsFrom(objectMapper.readTree(""))
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(udtCodec1)
        .convertsTo(
            udt1.newValue().setInt("f1a", 42).setMap("f1b", newMap("foo", 1234.56d, "bar", 0.12d)))
        .from(objectMapper.readTree("{\"f1a\":42,\"f1b\":{\"foo\":1234.56,\"bar\":0.12}}"))
        .convertsTo(udt1.newValue())
        .from(objectMapper.readTree("{\"f1a\":null,\"f1b\":{}}"))
        .convertsTo(null)
        .from(objectMapper.getNodeFactory().nullNode());
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(udtCodec1)
        .cannotConvertFrom(objectMapper.readTree("{\"f1a\":42}"))
        .cannotConvertFrom(objectMapper.readTree("[42]"))
        .cannotConvertFrom(objectMapper.readTree("{\"not a valid input\":\"foo\"}"));
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
