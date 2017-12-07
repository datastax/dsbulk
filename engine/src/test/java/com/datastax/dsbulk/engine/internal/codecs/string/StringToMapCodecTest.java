/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.EngineAssertions.assertThat;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToListCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToMapCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToStringCodec;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StringToMapCodecTest {

  private final ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  private final StringToDoubleCodec keyCodec =
      new StringToDoubleCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))),
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH,
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO));

  private final TypeCodec<List<String>> stringListCodec = TypeCodec.list(TypeCodec.varchar());

  private final ConvertingCodec<JsonNode, List<String>> valueCodec =
      new JsonNodeToListCodec<>(
          stringListCodec, new JsonNodeToStringCodec(TypeCodec.varchar()), objectMapper);

  private final TypeCodec<Map<Double, List<String>>> mapCodec =
      TypeCodec.map(TypeCodec.cdouble(), stringListCodec);

  private final StringToMapCodec<Double, List<String>> codec =
      new StringToMapCodec<>(
          new JsonNodeToMapCodec<>(mapCodec, keyCodec, valueCodec, objectMapper), objectMapper);

  @Test
  void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom("{1 : [\"foo\", \"bar\"], 2:[\"qix\"]}")
        .to(map(1d, list("foo", "bar"), 2d, list("qix")))
        .convertsFrom("{ '1234.56' : ['foo', 'bar'], '0.12' : ['qix'] }")
        .to(map(1234.56d, list("foo", "bar"), 0.12d, list("qix")))
        .convertsFrom("{ '1,234.56' : ['foo'] , '.12' : ['bar']}")
        .to(map(1234.56d, list("foo"), 0.12d, list("bar")))
        .convertsFrom("{1: , '' :['foo']}")
        .to(map(1d, null, null, list("foo")))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("{}")
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(map(1d, list("foo", "bar"), 2d, list("qix")))
        .from("{\"1\":[\"foo\",\"bar\"],\"2\":[\"qix\"]}")
        .convertsTo(map(1234.56d, list("foo", "bar"), 0.12d, list("qix")))
        .from("{\"1,234.56\":[\"foo\",\"bar\"],\"0.12\":[\"qix\"]}")
        .convertsTo(map(1d, null, 2d, list()))
        .from("{\"1\":null,\"2\":[]}")
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec)
        .cannotConvertFrom("not a valid input")
        .cannotConvertFrom("{not a,valid input}");
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
