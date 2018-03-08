/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;
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
import com.datastax.dsbulk.engine.internal.codecs.string.StringToDoubleCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
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

class JsonNodeToMapCodecTest {

  private final ObjectMapper objectMapper = CodecSettings.getObjectMapper();

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private final List<String> nullWords = newArrayList("NULL");

  private final ConvertingCodec<String, Double> keyCodec =
      new StringToDoubleCodec(
          numberFormat,
          OverflowStrategy.REJECT,
          RoundingMode.HALF_EVEN,
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO),
          nullWords);

  private final TypeCodec<List<String>> stringListCodec = TypeCodec.list(TypeCodec.varchar());

  private final JsonNodeToListCodec<String> valueCodec =
      new JsonNodeToListCodec<>(
          stringListCodec,
          new JsonNodeToStringCodec(TypeCodec.varchar(), nullWords),
          objectMapper,
          nullWords);

  private final TypeCodec<Map<Double, List<String>>> mapCodec =
      TypeCodec.map(TypeCodec.cdouble(), stringListCodec);

  private final JsonNodeToMapCodec<Double, List<String>> codec =
      new JsonNodeToMapCodec<>(mapCodec, keyCodec, valueCodec, objectMapper, nullWords);

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
        .convertsFromExternal(objectMapper.readTree("{1: , '' :['foo']}"))
        .toInternal(map(1d, null, null, list("foo")))
        .convertsFromExternal(objectMapper.readTree("{1: [\"NULL\"], 2: ['NULL']}"))
        .toInternal(map(1d, list((String) null), 2d, list((String) null)))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(objectMapper.readTree("{}"))
        .toInternal(null)
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
        .convertsFromInternal(map(1d, null, 2d, list()))
        .toExternal(objectMapper.readTree("{\"1\":null,\"2\":[]}"))
        .convertsFromInternal(null)
        .toExternal(objectMapper.getNodeFactory().nullNode());
  }

  @Test
  void should_not_convert_from_invalid_external() throws Exception {
    assertThat(codec)
        .cannotConvertFromExternal(objectMapper.readTree("{\"not a valid input\":\"foo\"}"));
    assertThat(codec)
        .cannotConvertFromExternal(objectMapper.readTree("[1,\"not a valid object\"]"));
    assertThat(codec).cannotConvertFromExternal(objectMapper.readTree("42"));
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
