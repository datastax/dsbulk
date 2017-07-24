/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.util.UUID;
import org.junit.Test;

import static com.datastax.driver.core.ProtocolVersion.V4;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class StringToUUIDCodecTest {

  @Test
  public void should_serialize_when_valid_input() throws Exception {
    StringToUUIDCodec codec = new StringToUUIDCodec(TypeCodec.uuid());
    assertSerde(codec, UUID.randomUUID().toString());
  }

  @Test
  public void should_not_serialize_when_invalid_input() throws Exception {
    StringToUUIDCodec codec = new StringToUUIDCodec(TypeCodec.uuid());
    try {
      assertSerde(codec, "not a valid uuid");
      fail("Expecting InvalidTypeException");
    } catch (InvalidTypeException ignored) {
    }
  }

  private void assertSerde(StringToUUIDCodec codec, String input) {
    assertThat(codec.deserialize(codec.serialize(input, V4), V4)).isEqualTo(input);
  }
}
