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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StringToBooleanCodecTest {

  private final Map<String, Boolean> inputs =
      ImmutableMap.<String, Boolean>builder().put("foo", true).put("bar", false).build();
  private final Map<Boolean, String> outputs =
      ImmutableMap.<Boolean, String>builder().put(true, "foo").put(false, "bar").build();

  private final StringToBooleanCodec codec = new StringToBooleanCodec(inputs, outputs);

  @Test
  void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom("FOO")
        .to(true)
        .convertsFrom("BAR")
        .to(false)
        .convertsFrom("foo")
        .to(true)
        .convertsFrom("bar")
        .to(false)
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(true)
        .from("foo")
        .convertsTo(false)
        .from("bar")
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec).cannotConvertFrom("not a valid boolean");
  }
}
