/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.EngineAssertions.assertThat;

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
