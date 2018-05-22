/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.temporal;

import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.FIXED;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MAX;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.MIN;
import static com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator.RANDOM;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import java.time.Instant;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

class TemporalToUUIDCodecTest {

  private TemporalToTemporalCodec<ZonedDateTime, Instant> instantCodec =
      new TemporalToTemporalCodec<>(
          ZonedDateTime.class, InstantCodec.instance, UTC, EPOCH.atZone(UTC));

  @Test
  void should_convert_when_valid_input() {

    assertThat(new TemporalToUUIDCodec<>(TypeCodec.timeUUID(), instantCodec, MIN))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(
            new TemporalToUUIDCodec<>(TypeCodec.timeUUID(), instantCodec, MIN)
                .externalToInternal(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")))
        .isEqualTo(
            UUIDs.startOf(
                ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli()));

    assertThat(
            new TemporalToUUIDCodec<>(TypeCodec.timeUUID(), instantCodec, MAX)
                .externalToInternal(ZonedDateTime.parse("2010-06-30T00:00:00.999999999+01:00")))
        .isEqualTo(
            UUIDs.endOf(
                ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00").toInstant().toEpochMilli()));

    assertThat(
            new TemporalToUUIDCodec<>(TypeCodec.timeUUID(), instantCodec, FIXED)
                .externalToInternal(ZonedDateTime.parse("2010-06-30T00:00:00.999999999+01:00"))
                .timestamp())
        .isEqualTo(
            UUIDs.endOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999999999+01:00")
                        .toInstant()
                        .toEpochMilli())
                .timestamp());

    assertThat(
            new TemporalToUUIDCodec<>(TypeCodec.timeUUID(), instantCodec, RANDOM)
                .externalToInternal(ZonedDateTime.parse("2010-06-30T00:00:00.999999999+01:00"))
                .timestamp())
        .isEqualTo(
            UUIDs.endOf(
                    ZonedDateTime.parse("2010-06-30T00:00:00.999999999+01:00")
                        .toInstant()
                        .toEpochMilli())
                .timestamp());

    assertThat(
            new TemporalToUUIDCodec<>(TypeCodec.timeUUID(), instantCodec, MIN)
                .internalToExternal(
                    UUIDs.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00"));

    assertThat(
            new TemporalToUUIDCodec<>(TypeCodec.timeUUID(), instantCodec, MAX)
                .internalToExternal(
                    UUIDs.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00"));

    assertThat(
            new TemporalToUUIDCodec<>(TypeCodec.timeUUID(), instantCodec, FIXED)
                .internalToExternal(
                    UUIDs.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00"));

    assertThat(
            new TemporalToUUIDCodec<>(TypeCodec.timeUUID(), instantCodec, RANDOM)
                .internalToExternal(
                    UUIDs.startOf(
                        ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00")
                            .toInstant()
                            .toEpochMilli())))
        .isEqualTo(ZonedDateTime.parse("2010-06-30T00:00:00.999+01:00"));
  }
}
