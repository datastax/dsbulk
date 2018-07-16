/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.driver.core.DataType.cdouble;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.dsbulk.engine.internal.codecs.CodecTestUtils.newCodecRegistry;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;

import com.datastax.driver.core.DataType;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToMapCodecTest {

  private StringToMapCodec<Double, List<String>> codec;

  @BeforeEach
  void setUp() {
    codec =
        (StringToMapCodec<Double, List<String>>)
            newCodecRegistry("nullStrings = [NULL], formatNumbers = true")
                .codecFor(
                    DataType.map(cdouble(), DataType.list(varchar())), TypeToken.of(String.class));
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("{1 : [\"foo\", \"bar\"], 2:[\"qix\"]}")
        .toInternal(map(1d, list("foo", "bar"), 2d, list("qix")))
        .convertsFromExternal("1 : [\"foo\", \"bar\"], 2:[\"qix\"]")
        .toInternal(map(1d, list("foo", "bar"), 2d, list("qix")))
        .convertsFromExternal("{ '1234.56' : ['foo', 'bar'], '0.12' : ['qix'] }")
        .toInternal(map(1234.56d, list("foo", "bar"), 0.12d, list("qix")))
        .convertsFromExternal("{ '1,234.56' : ['foo'] , '.12' : ['bar']}")
        .toInternal(map(1234.56d, list("foo"), 0.12d, list("bar")))
        .convertsFromExternal("{1: [], 2 :[]}")
        .toInternal(map(1d, list(), 2d, list()))
        // DAT-297: don't apply nullStrings to inner elements
        .convertsFromExternal("{1: [\"NULL\"], 2: ['NULL']}")
        .toInternal(map(1d, list("NULL"), 2d, list("NULL")))
        .convertsFromExternal("{1: [\"\"], 2: ['']}")
        .toInternal(map(1d, list(""), 2d, list("")))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null)
        .convertsFromExternal("{}")
        .toInternal(ImmutableMap.of())
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
        .convertsFromInternal(map(1d, list(""), 2d, list("")))
        .toExternal("{\"1\":[\"\"],\"2\":[\"\"]}")
        .convertsFromInternal(map(1d, null, 2d, list()))
        .toExternal("{\"1\":null,\"2\":[]}")
        .convertsFromInternal(ImmutableMap.of())
        .toExternal("{}")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal("{\"not a valid input\":\"foo\"}")
        .cannotConvertFromExternal("[1,\"not a valid object\"]")
        .cannotConvertFromExternal("42");
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
