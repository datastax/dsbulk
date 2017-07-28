/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import static com.datastax.driver.core.ProtocolVersion.V4;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.junit.Test;

public class StringToLocalDateCodecTest {

  @Test
  public void should_serialize_when_valid_iso_input() throws Exception {
    StringToLocalDateCodec codec = new StringToLocalDateCodec(ISO_LOCAL_DATE);
    assertSerde(codec, "2016-07-24", LocalDate.parse("2016-07-24"));
  }

  @Test
  public void should_serialize_when_valid_pattern_input() throws Exception {
    StringToLocalDateCodec codec =
        new StringToLocalDateCodec(DateTimeFormatter.ofPattern("yyyyMMdd"));
    assertSerde(codec, "20160724", LocalDate.parse("2016-07-24"));
  }

  @Test
  public void should_not_serialize_when_invalid_input() throws Exception {
    StringToLocalDateCodec codec = new StringToLocalDateCodec(ISO_LOCAL_DATE);
    try {
      assertSerde(codec, "not a valid date format", null);
      fail("Expecting InvalidTypeException");
    } catch (InvalidTypeException ignored) {
    }
  }

  private void assertSerde(StringToLocalDateCodec codec, String input, LocalDate expected) {
    assertThat(LocalDateCodec.instance.deserialize(codec.serialize(input, V4), V4))
        .isEqualTo(expected);
  }
}
