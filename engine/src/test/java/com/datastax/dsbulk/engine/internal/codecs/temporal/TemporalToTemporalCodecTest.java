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

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
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
                ZonedDateTime.class, TypeCodecs.DATE, UTC, EPOCH.atZone(UTC)))
        .convertsFromExternal(ZonedDateTime.parse("2010-06-30T00:00:00+01:00"))
        .toInternal(LocalDate.parse("2010-06-30"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                ZonedDateTime.class, TypeCodecs.DATE, ofHours(1), EPOCH.atZone(UTC)))
        .convertsFromExternal(ZonedDateTime.parse("2010-06-30T00:00:00+01:00"))
        .toInternal(LocalDate.parse("2010-06-30"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                ZonedDateTime.class, TypeCodecs.DATE, ofHours(-1), EPOCH.atZone(UTC)))
        .convertsFromExternal(ZonedDateTime.parse("2010-06-30T00:00:00+01:00"))
        .toInternal(LocalDate.parse("2010-06-30"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                ZonedDateTime.class, TypeCodecs.TIME, UTC, EPOCH.atZone(UTC)))
        .convertsFromExternal(ZonedDateTime.parse("1970-01-01T23:59:59+01:00"))
        .toInternal(LocalTime.parse("23:59:59"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                ZonedDateTime.class, TypeCodecs.TIME, ofHours(1), EPOCH.atZone(UTC)))
        .convertsFromExternal(ZonedDateTime.parse("1970-01-01T23:59:59+01:00"))
        .toInternal(LocalTime.parse("23:59:59"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                ZonedDateTime.class, TypeCodecs.TIME, ofHours(-1), EPOCH.atZone(UTC)))
        .convertsFromExternal(ZonedDateTime.parse("1970-01-01T23:59:59+01:00"))
        .toInternal(LocalTime.parse("23:59:59"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                ZonedDateTime.class, TypeCodecs.TIMESTAMP, UTC, EPOCH.atZone(UTC)))
        .convertsFromExternal(ZonedDateTime.parse("2010-06-30T00:00:00+01:00"))
        .toInternal(Instant.parse("2010-06-29T23:00:00Z"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                ZonedDateTime.class, TypeCodecs.TIMESTAMP, ofHours(1), EPOCH.atZone(UTC)))
        .convertsFromExternal(ZonedDateTime.parse("2010-06-30T00:00:00+01:00"))
        .toInternal(Instant.parse("2010-06-29T23:00:00Z"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    // Instant -> *

    assertThat(
            new TemporalToTemporalCodec<>(Instant.class, TypeCodecs.DATE, UTC, EPOCH.atZone(UTC)))
        .convertsFromExternal(Instant.parse("2010-06-30T00:00:00Z"))
        .toInternal(LocalDate.parse("2010-06-30"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                Instant.class, TypeCodecs.DATE, ofHours(-1), EPOCH.atZone(UTC)))
        .convertsFromExternal(Instant.parse("2010-06-30T00:00:00Z"))
        .toInternal(LocalDate.parse("2010-06-29"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                Instant.class, TypeCodecs.DATE, ofHours(1), EPOCH.atZone(UTC)))
        .convertsFromExternal(Instant.parse("2010-06-30T23:59:59Z"))
        .toInternal(LocalDate.parse("2010-07-01"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new TemporalToTemporalCodec<>(Instant.class, TypeCodecs.TIME, UTC, EPOCH.atZone(UTC)))
        .convertsFromExternal(Instant.parse("1970-01-01T23:59:59Z"))
        .toInternal(LocalTime.parse("23:59:59"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                Instant.class, TypeCodecs.TIME, ofHours(1), EPOCH.atZone(UTC)))
        .convertsFromExternal(Instant.parse("1970-01-01T23:59:59Z"))
        .toInternal(LocalTime.parse("00:59:59"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                Instant.class, TypeCodecs.TIME, ofHours(-1), EPOCH.atZone(UTC)))
        .convertsFromExternal(Instant.parse("1970-01-01T23:59:59Z"))
        .toInternal(LocalTime.parse("22:59:59"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    // LocalDateTime -> *

    assertThat(
            new TemporalToTemporalCodec<>(
                LocalDateTime.class, TypeCodecs.DATE, UTC, EPOCH.atZone(UTC)))
        .convertsFromExternal(LocalDateTime.parse("2010-06-30T00:00:00"))
        .toInternal(LocalDate.parse("2010-06-30"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                LocalDateTime.class, TypeCodecs.TIME, UTC, EPOCH.atZone(UTC)))
        .convertsFromExternal(LocalDateTime.parse("1970-01-01T23:59:59"))
        .toInternal(LocalTime.parse("23:59:59"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new TemporalToTemporalCodec<>(
                LocalDateTime.class, TypeCodecs.TIMESTAMP, UTC, EPOCH.atZone(UTC)))
        .convertsFromExternal(LocalDateTime.parse("2010-06-30T23:59:59"))
        .toInternal(Instant.parse("2010-06-30T23:59:59Z"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    // LocalDate -> *

    assertThat(
            new TemporalToTemporalCodec<>(
                LocalDate.class, TypeCodecs.TIMESTAMP, UTC, EPOCH.atZone(UTC)))
        .convertsFromExternal(LocalDate.parse("2010-06-30"))
        .toInternal(Instant.parse("2010-06-30T00:00:00Z"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    // LocalTime -> *

    assertThat(
            new TemporalToTemporalCodec<>(
                LocalTime.class, TypeCodecs.TIMESTAMP, UTC, EPOCH.atZone(UTC)))
        .convertsFromExternal(LocalTime.parse("23:59:59"))
        .toInternal(Instant.parse("1970-01-01T23:59:59Z"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_when_invalid_input() {

    // LocalDate <-> LocalTime is not supported
    assertThat(
            new TemporalToTemporalCodec<>(LocalDate.class, TypeCodecs.TIME, UTC, EPOCH.atZone(UTC)))
        .cannotConvertFromExternal(LocalDate.parse("2010-06-30"))
        .cannotConvertFromInternal(LocalTime.parse("23:59:59"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }
}
