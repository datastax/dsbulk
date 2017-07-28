/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.datastax.loader.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.junit.Test;

public class StringToInstantCodecTest {

  @Test
  public void should_serialize_when_valid_cql_input() throws Exception {
    StringToInstantCodec codec = new StringToInstantCodec(CQL_DATE_TIME_FORMAT);
    assertSerde(codec, "2016-07-24T20:34", Instant.parse("2016-07-24T20:34:00Z"));
    assertSerde(codec, "2016-07-24T20:34:12", Instant.parse("2016-07-24T20:34:12Z"));
    assertSerde(codec, "2016-07-24T20:34:12.999", Instant.parse("2016-07-24T20:34:12.999Z"));
    assertSerde(codec, "2016-07-24T20:34+01:00", Instant.parse("2016-07-24T19:34:00Z"));
    assertSerde(codec, "2016-07-24T20:34:12.999+01:00", Instant.parse("2016-07-24T19:34:12.999Z"));
  }

  @Test
  public void should_serialize_when_valid_pattern_input() throws Exception {
    StringToInstantCodec codec =
        new StringToInstantCodec(
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("UTC")));
    assertSerde(codec, "20160724203412", Instant.parse("2016-07-24T20:34:12Z"));
  }

  @Test
  public void should_not_serialize_when_invalid_input() throws Exception {
    StringToInstantCodec codec = new StringToInstantCodec(CQL_DATE_TIME_FORMAT);
    try {
      assertSerde(codec, "not a valid date format", null);
      fail("Expecting InvalidTypeException");
    } catch (InvalidTypeException ignored) {
    }
  }

  private void assertSerde(StringToInstantCodec codec, String input, Instant expected) {
    assertThat(InstantCodec.instance.deserialize(codec.serialize(input, V4), V4))
        .isEqualTo(expected);
  }
}
