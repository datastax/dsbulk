/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import static com.datastax.driver.core.ProtocolVersion.V4;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.extras.codecs.jdk8.LocalTimeCodec;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import org.junit.Test;

public class StringToLocalTimeCodecTest {

  @Test
  public void should_serialize_when_valid_iso_input() throws Exception {
    StringToLocalTimeCodec codec = new StringToLocalTimeCodec(ISO_LOCAL_TIME);
    assertSerde(codec, "12:24:46", LocalTime.parse("12:24:46"));
    assertSerde(codec, "12:24:46.999", LocalTime.parse("12:24:46.999"));
  }

  @Test
  public void should_serialize_when_valid_pattern_input() throws Exception {
    StringToLocalTimeCodec codec =
        new StringToLocalTimeCodec(DateTimeFormatter.ofPattern("HHmmss.SSS"));
    assertSerde(codec, "122446.999", LocalTime.parse("12:24:46.999"));
  }

  @Test
  public void should_not_serialize_when_invalid_input() throws Exception {
    StringToLocalTimeCodec codec = new StringToLocalTimeCodec(ISO_LOCAL_TIME);
    try {
      assertSerde(codec, "not a valid time format", null);
      fail("Expecting InvalidTypeException");
    } catch (InvalidTypeException ignored) {
    }
  }

  private void assertSerde(StringToLocalTimeCodec codec, String input, LocalTime expected) {
    assertThat(LocalTimeCodec.instance.deserialize(codec.serialize(input, V4), V4))
        .isEqualTo(expected);
  }
}
