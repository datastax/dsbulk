/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Test;

import static com.datastax.driver.core.ProtocolVersion.V4;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class StringToBooleanCodecTest {

  Map<String, Boolean> inputs =
      ImmutableMap.<String, Boolean>builder().put("foo", true).put("bar", false).build();
  Map<Boolean, String> outputs =
      ImmutableMap.<Boolean, String>builder().put(true, "foo").put(false, "bar").build();

  @Test
  public void should_serialize_when_valid_input() throws Exception {
    StringToBooleanCodec codec = new StringToBooleanCodec(inputs, outputs);
    assertSerde(codec, "FOO");
    assertSerde(codec, "BAR");
    assertSerde(codec, "Foo");
    assertSerde(codec, "Bar");
    assertSerde(codec, null);
  }

  @Test
  public void should_not_serialize_when_invalid_input() throws Exception {
    StringToBooleanCodec codec = new StringToBooleanCodec(inputs, outputs);
    try {
      assertSerde(codec, "qix");
      fail("Expecting InvalidTypeException");
    } catch (InvalidTypeException ignored) {
    }
  }

  private void assertSerde(StringToBooleanCodec codec, String input) {
    assertThat(codec.deserialize(codec.serialize(input, V4), V4))
        .isEqualTo(input == null ? null : input.toLowerCase());
  }
}
