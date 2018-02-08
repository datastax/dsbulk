/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.temporal;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.time.ZoneOffset.ofHours;

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
  void should_convert_when_valid_input() {

    // ZDT -> *

    assertThat(
            new TemporalToTemporalCodec<>(
                ZonedDateTime.class, LocalDateCodec.instance, UTC, EPOCH.atZone(UTC)))
        .convertsFrom(ZonedDateTime.parse("2010-06-30T00:00:00+01:00"))
        .to(LocalDate.parse("2010-06-30"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                ZonedDateTime.class, LocalDateCodec.instance, ofHours(1), EPOCH.atZone(UTC)))
        .convertsFrom(ZonedDateTime.parse("2010-06-30T00:00:00+01:00"))
        .to(LocalDate.parse("2010-06-30"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                ZonedDateTime.class, LocalDateCodec.instance, ofHours(-1), EPOCH.atZone(UTC)))
        .convertsFrom(ZonedDateTime.parse("2010-06-30T00:00:00+01:00"))
        .to(LocalDate.parse("2010-06-30"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                ZonedDateTime.class, LocalTimeCodec.instance, UTC, EPOCH.atZone(UTC)))
        .convertsFrom(ZonedDateTime.parse("1970-01-01T23:59:59+01:00"))
        .to(LocalTime.parse("23:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                ZonedDateTime.class, LocalTimeCodec.instance, ofHours(1), EPOCH.atZone(UTC)))
        .convertsFrom(ZonedDateTime.parse("1970-01-01T23:59:59+01:00"))
        .to(LocalTime.parse("23:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                ZonedDateTime.class, LocalTimeCodec.instance, ofHours(-1), EPOCH.atZone(UTC)))
        .convertsFrom(ZonedDateTime.parse("1970-01-01T23:59:59+01:00"))
        .to(LocalTime.parse("23:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                ZonedDateTime.class, InstantCodec.instance, UTC, EPOCH.atZone(UTC)))
        .convertsFrom(ZonedDateTime.parse("2010-06-30T00:00:00+01:00"))
        .to(Instant.parse("2010-06-29T23:00:00Z"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                ZonedDateTime.class, InstantCodec.instance, ofHours(1), EPOCH.atZone(UTC)))
        .convertsFrom(ZonedDateTime.parse("2010-06-30T00:00:00+01:00"))
        .to(Instant.parse("2010-06-29T23:00:00Z"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    // Instant -> *

    assertThat(
            new TemporalToTemporalCodec<>(
                Instant.class, LocalDateCodec.instance, UTC, EPOCH.atZone(UTC)))
        .convertsFrom(Instant.parse("2010-06-30T00:00:00Z"))
        .to(LocalDate.parse("2010-06-30"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                Instant.class, LocalDateCodec.instance, ofHours(-1), EPOCH.atZone(UTC)))
        .convertsFrom(Instant.parse("2010-06-30T00:00:00Z"))
        .to(LocalDate.parse("2010-06-29"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                Instant.class, LocalDateCodec.instance, ofHours(1), EPOCH.atZone(UTC)))
        .convertsFrom(Instant.parse("2010-06-30T23:59:59Z"))
        .to(LocalDate.parse("2010-07-01"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                Instant.class, LocalTimeCodec.instance, UTC, EPOCH.atZone(UTC)))
        .convertsFrom(Instant.parse("1970-01-01T23:59:59Z"))
        .to(LocalTime.parse("23:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                Instant.class, LocalTimeCodec.instance, ofHours(1), EPOCH.atZone(UTC)))
        .convertsFrom(Instant.parse("1970-01-01T23:59:59Z"))
        .to(LocalTime.parse("00:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                Instant.class, LocalTimeCodec.instance, ofHours(-1), EPOCH.atZone(UTC)))
        .convertsFrom(Instant.parse("1970-01-01T23:59:59Z"))
        .to(LocalTime.parse("22:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    // LocalDateTime -> *

    assertThat(
            new TemporalToTemporalCodec<>(
                LocalDateTime.class, LocalDateCodec.instance, UTC, EPOCH.atZone(UTC)))
        .convertsFrom(LocalDateTime.parse("2010-06-30T00:00:00"))
        .to(LocalDate.parse("2010-06-30"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                LocalDateTime.class, LocalTimeCodec.instance, UTC, EPOCH.atZone(UTC)))
        .convertsFrom(LocalDateTime.parse("1970-01-01T23:59:59"))
        .to(LocalTime.parse("23:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                LocalDateTime.class, InstantCodec.instance, UTC, EPOCH.atZone(UTC)))
        .convertsFrom(LocalDateTime.parse("2010-06-30T23:59:59"))
        .to(Instant.parse("2010-06-30T23:59:59Z"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    // LocalDate -> *

    assertThat(
            new TemporalToTemporalCodec<>(
                LocalDate.class, InstantCodec.instance, UTC, EPOCH.atZone(UTC)))
        .convertsFrom(LocalDate.parse("2010-06-30"))
        .to(Instant.parse("2010-06-30T00:00:00Z"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    // LocalTime -> *

    assertThat(
            new TemporalToTemporalCodec<>(
                LocalTime.class, InstantCodec.instance, UTC, EPOCH.atZone(UTC)))
        .convertsFrom(LocalTime.parse("23:59:59"))
        .to(Instant.parse("1970-01-01T23:59:59Z"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_not_convert_when_invalid_input() {

    // LocalDate <-> LocalTime is not supported
    assertThat(
            new TemporalToTemporalCodec<>(
                LocalDate.class, LocalTimeCodec.instance, UTC, EPOCH.atZone(UTC)))
        .cannotConvertFrom(LocalDate.parse("2010-06-30"))
        .cannotConvertTo(LocalTime.parse("23:59:59"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);
  }
}
