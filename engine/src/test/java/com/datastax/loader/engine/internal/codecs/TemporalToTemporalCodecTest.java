/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import com.datastax.driver.extras.codecs.jdk8.LocalTimeCodec;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import org.junit.Test;

import static com.datastax.driver.core.ProtocolVersion.V4;
import static org.assertj.core.api.Assertions.assertThat;

public class TemporalToTemporalCodecTest {

  @Test
  public void should_serialize_when_valid_input() throws Exception {

    assertSerde(
        new TemporalToTemporalCodec<>(ZonedDateTime.class, LocalDateCodec.instance),
        ZonedDateTime.parse("2010-06-30T00:00:00Z"));
    assertSerde(
        new TemporalToTemporalCodec<>(ZonedDateTime.class, LocalTimeCodec.instance),
        ZonedDateTime.parse("1970-01-01T23:59:59Z"));
    assertSerde(
        new TemporalToTemporalCodec<>(ZonedDateTime.class, InstantCodec.instance),
        ZonedDateTime.parse("2010-06-30T23:59:59Z"));

    assertSerde(
        new TemporalToTemporalCodec<>(Instant.class, LocalDateCodec.instance),
        Instant.parse("2010-06-30T00:00:00Z"));
    assertSerde(
        new TemporalToTemporalCodec<>(Instant.class, LocalTimeCodec.instance),
        Instant.parse("1970-01-01T23:59:59Z"));

    assertSerde(
        new TemporalToTemporalCodec<>(LocalDateTime.class, LocalDateCodec.instance),
        LocalDateTime.parse("2010-06-30T00:00:00"));
    assertSerde(
        new TemporalToTemporalCodec<>(LocalDateTime.class, LocalTimeCodec.instance),
        LocalDateTime.parse("1970-01-01T23:59:59"));
    assertSerde(
        new TemporalToTemporalCodec<>(LocalDateTime.class, InstantCodec.instance),
        LocalDateTime.parse("2010-06-30T23:59:59"));

    assertSerde(
        new TemporalToTemporalCodec<>(LocalDate.class, InstantCodec.instance),
        LocalDate.parse("2010-06-30"));
    assertSerde(
        new TemporalToTemporalCodec<>(LocalTime.class, InstantCodec.instance),
        LocalTime.parse("23:59:59"));
  }

  private <FROM extends TemporalAccessor, TO extends TemporalAccessor> void assertSerde(
      TemporalToTemporalCodec<FROM, TO> codec, FROM input) {
    assertThat(codec.deserialize(codec.serialize(input, V4), V4)).isEqualTo(input);
  }
}
