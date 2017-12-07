/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.driver.core.DataType.cdouble;
import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.date;
import static com.datastax.driver.core.DataType.list;
import static com.datastax.driver.core.DataType.map;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.DriverCoreEngineTestHooks.newField;
import static com.datastax.driver.core.DriverCoreEngineTestHooks.newUserType;
import static com.datastax.driver.core.TypeCodec.userType;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToDoubleCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToIntegerCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToListCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToLocalDateCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToMapCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToUDTCodec;
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
import org.junit.jupiter.api.Test;

class StringToUDTCodecTest {

  private final ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  private final CodecRegistry codecRegistry = new CodecRegistry().register(LocalDateCodec.instance);

  private final ThreadLocal<DecimalFormat> formatter =
      ThreadLocal.withInitial(
          () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US)));

  // UDT 1

  private final UserType udt1 =
      newUserType(
          codecRegistry, newField("f1a", cint()), newField("f1b", map(varchar(), cdouble())));

  private final UDTValue udt1Empty = udt1.newValue().setToNull("f1a").setToNull("f1b");
  private final UDTValue udt1Value =
      udt1.newValue().setInt("f1a", 42).setMap("f1b", newMap("foo", 1234.56d, "bar", 0.12d));

  private final ConvertingCodec f1aCodec =
      new JsonNodeToIntegerCodec(
          formatter,
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH,
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO));
  private final ConvertingCodec f1bCodec =
      new JsonNodeToMapCodec<>(
          TypeCodec.map(TypeCodec.varchar(), TypeCodec.cdouble()),
          new StringToStringCodec(TypeCodec.varchar()),
          new JsonNodeToDoubleCodec(
              formatter,
              CQL_DATE_TIME_FORMAT,
              MILLISECONDS,
              EPOCH,
              ImmutableMap.of("true", true, "false", false),
              newArrayList(ONE, ZERO)),
          objectMapper);

  @SuppressWarnings("unchecked")
  private final StringToUDTCodec udtCodec1 =
      new StringToUDTCodec(
          new JsonNodeToUDTCodec(
              userType(udt1), ImmutableMap.of("f1a", f1aCodec, "f1b", f1bCodec), objectMapper),
          objectMapper);

  // UDT 2

  private final UserType udt2 =
      newUserType(codecRegistry, newField("f2a", udt1), newField("f2b", list(date())));

  private final UDTValue udt2Empty = udt2.newValue().setToNull("f2a").setToNull("f2b");
  private final UDTValue udt2Value =
      udt2.newValue()
          .setUDTValue("f2a", udt1Value)
          .set("f2b", newList(LocalDate.of(2017, 9, 22)), TypeCodec.list(LocalDateCodec.instance));

  @SuppressWarnings("unchecked")
  private final ConvertingCodec f2aCodec =
      new JsonNodeToUDTCodec(
          userType(udt1), ImmutableMap.of("f1a", f1aCodec, "f1b", f1bCodec), objectMapper);

  private final ConvertingCodec f2bCodec =
      new JsonNodeToListCodec<>(
          TypeCodec.list(LocalDateCodec.instance),
          new JsonNodeToLocalDateCodec(CQL_DATE_TIME_FORMAT),
          objectMapper);

  @SuppressWarnings("unchecked")
  private final StringToUDTCodec udtCodec2 =
      new StringToUDTCodec(
          new JsonNodeToUDTCodec(
              userType(udt2), ImmutableMap.of("f2a", f2aCodec, "f2b", f2bCodec), objectMapper),
          objectMapper);

  @Test
  void should_convert_from_valid_input() throws Exception {
    assertThat(udtCodec1)
        .convertsFrom("{\"f1a\":42,\"f1b\":{\"foo\":1234.56,\"bar\":0.12}}")
        .to(udt1Value)
        .convertsFrom("{'f1a':42,'f1b':{'foo':1234.56,'bar':0.12}}")
        .to(udt1Value)
        .convertsFrom(
            "{ \"f1b\" :  { \"foo\" : \"1,234.56\" , \"bar\" : \"0000.12000\" } , \"f1a\" : \"42.00\" }")
        .to(udt1Value)
        .convertsFrom("{ \"f1b\" :  null , \"f1a\" :  null }")
        .to(udt1Empty)
        .convertsFrom(null)
        .to(null)
        .convertsFrom("{}")
        .to(null)
        .convertsFrom("")
        .to(null);
    assertThat(udtCodec2)
        .convertsFrom(
            "{\"f2a\":{\"f1a\":42,\"f1b\":{\"foo\":1234.56,\"bar\":0.12}},\"f2b\":[\"2017-09-22\"]}")
        .to(udt2Value)
        .convertsFrom("{'f2a':{'f1a':42,'f1b':{'foo':1234.56,'bar':0.12}},'f2b':['2017-09-22']}")
        .to(udt2Value)
        .convertsFrom(
            "{ \"f2b\" :  [ \"2017-09-22\" ] , \"f2a\" : { \"f1b\" :  { \"foo\" : \"1,234.56\" , \"bar\" : \"0000.12000\" } , \"f1a\" : \"42.00\" } }")
        .to(udt2Value)
        .convertsFrom("{ \"f2b\" :  null , \"f2a\" :  null }")
        .to(udt2Empty)
        .convertsFrom(null)
        .to(null)
        .convertsFrom("{}")
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    assertThat(udtCodec1)
        .convertsTo(
            udt1.newValue().setInt("f1a", 42).setMap("f1b", newMap("foo", 1234.56d, "bar", 0.12d)))
        .from("{\"f1a\":42,\"f1b\":{\"foo\":1234.56,\"bar\":0.12}}")
        .convertsTo(udt1.newValue())
        .from("{\"f1a\":null,\"f1b\":{}}")
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_not_convert_from_invalid_input() throws Exception {
    assertThat(udtCodec1)
        .cannotConvertFrom("{\"f1a\":42}")
        .cannotConvertFrom("[42]")
        .cannotConvertFrom("{\"not a valid input\":\"foo\"}");
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
