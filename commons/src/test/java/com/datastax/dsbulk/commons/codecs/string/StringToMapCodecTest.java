/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.string;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;

import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistryBuilder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToMapCodecTest {

  private StringToMapCodec<Double, List<String>> codec;

  @BeforeEach
  @SuppressWarnings({"unchecked", "RedundantSuppression"})
  void setUp() {
    codec =
        (StringToMapCodec<Double, List<String>>)
            new ExtendedCodecRegistryBuilder()
                .withNullStrings("NULL")
                .withFormatNumbers(true)
                .build()
                .codecFor(
                    DataTypes.mapOf(DataTypes.DOUBLE, DataTypes.listOf(DataTypes.TEXT)),
                    GenericType.STRING);
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
