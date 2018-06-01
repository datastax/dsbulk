/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StringToBooleanCodecTest {

  private final Map<String, Boolean> inputs =
      ImmutableMap.<String, Boolean>builder().put("foo", true).put("bar", false).build();

  private final Map<Boolean, String> outputs =
      ImmutableMap.<Boolean, String>builder().put(true, "foo").put(false, "bar").build();

  private final StringToBooleanCodec codec =
      new StringToBooleanCodec(inputs, outputs, newArrayList("NULL"));

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("FOO")
        .toInternal(true)
        .convertsFromExternal("BAR")
        .toInternal(false)
        .convertsFromExternal("foo")
        .toInternal(true)
        .convertsFromExternal("bar")
        .toInternal(false)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(true)
        .toExternal("foo")
        .convertsFromInternal(false)
        .toExternal("bar")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec).cannotConvertFromExternal("not a valid boolean");
  }
}
