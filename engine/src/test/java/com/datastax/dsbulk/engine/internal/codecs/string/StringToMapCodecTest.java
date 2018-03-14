/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToListCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToMapCodec;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToStringCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.netty.util.concurrent.FastThreadLocal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StringToMapCodecTest {

  private final ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private final List<String> nullStrings = newArrayList("NULL");

  private final StringToDoubleCodec keyCodec =
      new StringToDoubleCodec(
          numberFormat,
          OverflowStrategy.REJECT,
          RoundingMode.HALF_EVEN,
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO),
          nullStrings);

  private final TypeCodec<List<String>> stringListCodec = TypeCodec.list(TypeCodec.varchar());

  private final ConvertingCodec<JsonNode, List<String>> valueCodec =
      new JsonNodeToListCodec<>(
          stringListCodec,
          new JsonNodeToStringCodec(TypeCodec.varchar(), nullStrings),
          objectMapper,
          nullStrings);

  private final TypeCodec<Map<Double, List<String>>> mapCodec =
      TypeCodec.map(TypeCodec.cdouble(), stringListCodec);

  private final StringToMapCodec<Double, List<String>> codec =
      new StringToMapCodec<>(
          new JsonNodeToMapCodec<>(mapCodec, keyCodec, valueCodec, objectMapper, nullStrings),
          objectMapper,
          nullStrings);

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("{1 : [\"foo\", \"bar\"], 2:[\"qix\"]}")
        .toInternal(map(1d, list("foo", "bar"), 2d, list("qix")))
        .convertsFromExternal("{ '1234.56' : ['foo', 'bar'], '0.12' : ['qix'] }")
        .toInternal(map(1234.56d, list("foo", "bar"), 0.12d, list("qix")))
        .convertsFromExternal("{ '1,234.56' : ['foo'] , '.12' : ['bar']}")
        .toInternal(map(1234.56d, list("foo"), 0.12d, list("bar")))
        .convertsFromExternal("{1: , '' :['foo']}")
        .toInternal(map(1d, null, null, list("foo")))
        .convertsFromExternal("{1: [\"NULL\"], 2: ['NULL']}")
        .toInternal(map(1d, list((String) null), 2d, list((String) null)))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("{}")
        .toInternal(null)
        .convertsFromExternal("")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(map(1d, list("foo", "bar"), 2d, list("qix")))
        .toExternal("{\"1\":[\"foo\",\"bar\"],\"2\":[\"qix\"]}")
        .convertsFromInternal(map(1234.56d, list("foo", "bar"), 0.12d, list("qix")))
        .toExternal("{\"1,234.56\":[\"foo\",\"bar\"],\"0.12\":[\"qix\"]}")
        .convertsFromInternal(map(1d, null, 2d, list()))
        .toExternal("{\"1\":null,\"2\":[]}")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal("not a valid input")
        .cannotConvertFromExternal("{not a,valid input}");
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
