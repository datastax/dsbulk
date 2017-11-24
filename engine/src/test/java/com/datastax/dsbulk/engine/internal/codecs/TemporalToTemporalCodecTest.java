/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs;

import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;

import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import com.datastax.driver.extras.codecs.jdk8.LocalTimeCodec;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

class TemporalToTemporalCodecTest {

  @Test
  void should_convert_when_valid_input() throws Exception {

    assertThat(new TemporalToTemporalCodec<>(ZonedDateTime.class, LocalDateCodec.instance))
        .convertsFrom(ZonedDateTime.parse("2010-06-30T00:00:00+01:00"))
        .to(LocalDate.parse("2010-06-30"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new TemporalToTemporalCodec<>(ZonedDateTime.class, LocalTimeCodec.instance))
        .convertsFrom(ZonedDateTime.parse("1970-01-01T23:59:59+01:00"))
        .to(LocalTime.parse("23:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new TemporalToTemporalCodec<>(ZonedDateTime.class, InstantCodec.instance))
        .convertsFrom(ZonedDateTime.parse("2010-06-30T00:00:00+01:00"))
        .to(Instant.parse("2010-06-29T23:00:00Z"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new TemporalToTemporalCodec<>(Instant.class, LocalDateCodec.instance))
        .convertsFrom(Instant.parse("2010-06-30T00:00:00Z"))
        .to(LocalDate.parse("2010-06-30"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new TemporalToTemporalCodec<>(Instant.class, LocalTimeCodec.instance))
        .convertsFrom(Instant.parse("1970-01-01T23:59:59Z"))
        .to(LocalTime.parse("23:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new TemporalToTemporalCodec<>(LocalDateTime.class, LocalDateCodec.instance))
        .convertsFrom(LocalDateTime.parse("2010-06-30T00:00:00"))
        .to(LocalDate.parse("2010-06-30"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new TemporalToTemporalCodec<>(LocalDateTime.class, LocalTimeCodec.instance))
        .convertsFrom(LocalDateTime.parse("1970-01-01T23:59:59"))
        .to(LocalTime.parse("23:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new TemporalToTemporalCodec<>(LocalDateTime.class, InstantCodec.instance))
        .convertsFrom(LocalDateTime.parse("2010-06-30T23:59:59"))
        .to(Instant.parse("2010-06-30T23:59:59Z"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new TemporalToTemporalCodec<>(LocalDate.class, InstantCodec.instance))
        .convertsFrom(LocalDate.parse("2010-06-30"))
        .to(Instant.parse("2010-06-30T00:00:00Z"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new TemporalToTemporalCodec<>(LocalTime.class, InstantCodec.instance))
        .convertsFrom(LocalTime.parse("23:59:59"))
        .to(Instant.parse("1970-01-01T23:59:59Z"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_not_convert_when_invalid_input() throws Exception {

    // LocalDate <-> LocalTime is not supported
    assertThat(new TemporalToTemporalCodec<>(LocalDate.class, LocalTimeCodec.instance))
        .cannotConvertFrom(LocalDate.parse("2010-06-30"))
        .cannotConvertTo(LocalTime.parse("23:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);
  }
}
